#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fs2::FileExt;
use threadlane_connectors::{ConnectorError, CredentialStore};
use threadlane_decisions::{
    classify_advice, list_advice_models, preview_advice, AdviceClassifyOutput, AdviceError,
    AdvicePreviewOutput, AdviceScope, AdviceSource, DecisionClient, DecisionError,
    DecisionProvider, DecisionTransport, TransportFuture, TransportMethod, TransportRequest,
    TransportResponse, ADVICE_CACHE_SCHEMA_VERSION, DEFAULT_CACHE_TTL_SECONDS, JEV_CLASSIFY_URL,
    OPENAI_CLASSIFY_URL,
};
use threadlane_registry::{
    DecisionAdvisor, DecisionAdvisorMode, Difficulty, ModelChoice, ModelDefinition, RegistryChange,
    RegistryStore, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
};

static CLI_TEST_SEQ: AtomicU64 = AtomicU64::new(1);

struct CliTestFixture {
    base: PathBuf,
    project_root: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    cache_dir: PathBuf,
}

impl CliTestFixture {
    fn new(label: &str) -> Self {
        let seq = CLI_TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let raw_tmp = fs::canonicalize(env::temp_dir()).unwrap_or_else(|_| env::temp_dir());
        let base = raw_tmp.join(format!(
            "threadlane-advice-cli-{label}-{}-{seq}",
            std::process::id()
        ));
        if base.exists() {
            let _ = fs::remove_dir_all(&base);
        }
        fs::create_dir_all(&base).expect("create test base");
        fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).expect("chmod 0700 base");

        let project_root = base.join("project");
        let registry_dir = base.join("registry");
        let plans_dir = base.join("plans");
        let cache_dir = plans_dir.join("advice-cache");

        for dir in [&project_root, &registry_dir, &plans_dir] {
            fs::create_dir_all(dir).expect("create dir");
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700 dir");
        }

        Self {
            base,
            project_root,
            registry_dir,
            plans_dir,
            cache_dir,
        }
    }

    fn seed_jev_registry(&self) -> u64 {
        let store = RegistryStore::open(self.registry_dir.clone()).expect("open registry");
        store
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "jev-api".to_string(),
                        label: "TypeSafe Jev".to_string(),
                        adapter: ToolAdapter::JevApi,
                        executable_path: None,
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool");
        let snap = store
            .apply(
                1,
                RegistryChange::UpsertModels {
                    models: vec![ModelDefinition {
                        id: "jev-api:jev-latest".to_string(),
                        tool_id: "jev-api".to_string(),
                        label: "jev-latest".to_string(),
                        model_arg: "jev-latest".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    }],
                },
            )
            .expect("upsert model");
        snap.revision
    }

    fn write_request(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.base.join(name);
        fs::write(&path, contents.as_bytes()).expect("write request");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod 0600 request");
        path
    }
}

impl Drop for CliTestFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.base);
    }
}

fn advice_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_threadlane-advice"))
}

#[test]
fn cli_help_and_version_run_offline_without_touching_filesystem() {
    let fixture = CliTestFixture::new("help-version");
    fs::remove_dir_all(&fixture.registry_dir).expect("remove registry_dir");
    fs::remove_dir_all(&fixture.plans_dir).expect("remove plans_dir");

    let help_out = Command::new(advice_bin())
        .arg("--help")
        .output()
        .expect("run --help");
    assert!(help_out.status.success());
    let help_stdout = String::from_utf8_lossy(&help_out.stdout);
    assert!(help_stdout.contains("threadlane-advice"));
    assert!(help_stdout.contains("Jev"));
    assert!(help_stdout.contains("OpenAI Decisions"));
    assert!(help_stdout.contains("--registry-dir"));
    assert!(help_stdout.contains("--cache-dir"));
    assert!(help_stdout.contains("--project-root"));
    assert!(help_stdout.contains("--context-id"));
    assert!(help_stdout.contains("--profile-id"));
    assert!(help_stdout.contains("preview"));
    assert!(help_stdout.contains("classify"));
    assert!(help_stdout.contains("models"));
    assert!(help_out.stderr.is_empty());

    let ver_out = Command::new(advice_bin())
        .arg("--version")
        .output()
        .expect("run --version");
    assert!(ver_out.status.success());
    let ver_stdout = String::from_utf8_lossy(&ver_out.stdout);
    assert!(ver_stdout.contains("threadlane-advice"));
    assert!(ver_out.stderr.is_empty());

    assert!(!fixture.registry_dir.exists());
    assert!(!fixture.plans_dir.exists());
}

#[test]
fn cli_preview_and_cached_classify_and_ownership_refusals_work_end_to_end() {
    let fixture = CliTestFixture::new("e2e-subprocess");
    let reg_rev = fixture.seed_jev_registry();
    let context_id = "550e8400-e29b-41d4-a716-446655440000";
    let raw_summary = "Subprocess test summary that must never appear in classify output.";

    let req_path = fixture.write_request(
        "request.json",
        &format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "cli-task-1",
                "task_revision": 1,
                "task_summary": "{raw_summary}",
                "baseline_difficulty": "easy"
            }}"#
        ),
    );

    // 1. Run `preview` via real subprocess and verify cache_dir is NOT created.
    let preview_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run preview subprocess");
    assert!(
        preview_cmd.status.success(),
        "preview failed: {}",
        String::from_utf8_lossy(&preview_cmd.stderr)
    );
    assert!(
        !fixture.cache_dir.exists(),
        "preview subprocess must not create cache_dir"
    );

    let preview_stdout = String::from_utf8_lossy(&preview_cmd.stdout);
    assert!(
        !preview_stdout.contains("profile_id"),
        "preview output without --profile-id must omit profile_id"
    );
    let preview: AdvicePreviewOutput =
        serde_json::from_slice(&preview_cmd.stdout).expect("parse preview json");
    assert!(preview.untrusted_data);
    assert!(preview.advisory_only);
    assert_eq!(preview.mode, "shadow");
    assert_eq!(preview.profile_id, None);
    assert_eq!(preview.task_summary, raw_summary);
    assert!(!preview.provider_call_performed);

    // 1b. Verify subprocess `preview` preserves leading/trailing whitespace in `task_summary`
    //     while computing identical `summary_sha256` and `cache_key`.
    let untrimmed_summary = format!("  \n\t {raw_summary} \t\n  ");
    let untrimmed_req_val = serde_json::json!({
        "schema_version": 1,
        "expected_registry_revision": reg_rev,
        "tool_id": "jev-api",
        "model_id": "jev-api:jev-latest",
        "task_id": "cli-task-1",
        "task_revision": 1,
        "task_summary": untrimmed_summary,
        "baseline_difficulty": "easy"
    });
    let untrimmed_req_path = fixture.write_request(
        "request-untrimmed.json",
        &serde_json::to_string(&untrimmed_req_val).expect("serialize untrimmed req"),
    );
    let untrimmed_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--request")
        .arg(&untrimmed_req_path)
        .output()
        .expect("run untrimmed preview subprocess");
    assert!(untrimmed_cmd.status.success());
    let untrimmed_preview: AdvicePreviewOutput =
        serde_json::from_slice(&untrimmed_cmd.stdout).expect("parse untrimmed preview json");
    assert_eq!(untrimmed_preview.task_summary, untrimmed_summary);
    assert_eq!(untrimmed_preview.summary_sha256, preview.summary_sha256);
    assert_eq!(untrimmed_preview.cache_key, preview.cache_key);
    assert!(!format!("{untrimmed_preview:?}").contains(raw_summary));

    // 2. Seed a `Completed` cache record for `preview.cache_key` and verify `classify` subprocess
    //    returns a cache hit with zero network/vault access and redacts raw summary.
    fs::create_dir_all(&fixture.cache_dir).expect("create cache_dir");
    fs::set_permissions(&fixture.cache_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 cache_dir");

    let prompt_digest = {
        // Compute prompt_digest by running a quick classify that aborts or by matching the schema
        // Wait: let's inspect the Pending record created by `classify` when we hold or trigger it,
        // or let's trigger a pre-API Pending state or read the prompt_digest from a Pending record!
        let lock_path = fixture
            .cache_dir
            .join(format!("{}.lock", preview.cache_key));
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .expect("create lock file");
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 lock file");
        lock_file.try_lock_exclusive().expect("lock exclusive");

        // While holding the lock, a subprocess `classify` must fail immediately with `cache_busy`!
        let busy_out = Command::new(advice_bin())
            .arg("--registry-dir")
            .arg(&fixture.registry_dir)
            .arg("--cache-dir")
            .arg(&fixture.cache_dir)
            .arg("--project-root")
            .arg(&fixture.project_root)
            .arg("--context-id")
            .arg(context_id)
            .arg("classify")
            .arg("--request")
            .arg(&req_path)
            .output()
            .expect("run busy classify");
        assert!(!busy_out.status.success());
        let busy_err: AdviceError =
            serde_json::from_slice(&busy_out.stderr).expect("parse busy error json");
        assert_eq!(busy_err.code(), "cache_busy");

        lock_file.unlock().expect("unlock");
        "verified_lock"
    };
    assert_eq!(prompt_digest, "verified_lock");

    // 3. Verify duplicate flags and secret-like invalid argv are rejected with sanitized JSON.
    let secret_arg = "sk-live-super-secret-argv-999999";
    let dup_out = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--context-id")
        .arg(secret_arg)
        .arg("preview")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run duplicate flag subprocess");
    assert!(!dup_out.status.success());
    let dup_stderr = String::from_utf8_lossy(&dup_out.stderr);
    assert!(
        !dup_stderr.contains(secret_arg),
        "stderr must never echo raw argv values"
    );
    let dup_err: AdviceError =
        serde_json::from_slice(&dup_out.stderr).expect("parse duplicate flag error");
    assert_eq!(dup_err.code(), "invalid_input");

    // 4. Seed a valid `Completed` cache record and verify subprocess `classify` serves it offline.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("unix epoch")
        .as_millis() as u64;
    let project_root_sha256 = threadlane_decisions::sha256_hex(preview.project_root.as_bytes());
    let prompt_digest_hex = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        for field in [
            "threadlane-jev-shadow-v1",
            "difficulty",
            "Select implementation difficulty from the supplied task evidence.",
            "Small isolated behavior-preserving edit.",
            "Bounded multi-step change within known module boundaries.",
            "Substantial uncertainty, concurrency, or cross-module integration complexity.",
            "High-risk architectural, security-critical, or multi-system redesign.",
        ] {
            h.update((field.len() as u64).to_le_bytes());
            h.update(field.as_bytes());
        }
        let digest = h.finalize();
        let mut s = String::with_capacity(64);
        for b in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut s, "{b:02x}");
        }
        s
    };

    let record_path = fixture
        .cache_dir
        .join(format!("{}.json", preview.cache_key));
    let completed_record_json = serde_json::json!({
        "schema_version": ADVICE_CACHE_SCHEMA_VERSION,
        "prompt_digest": prompt_digest_hex,
        "cache_key": preview.cache_key,
        "project_root_sha256": project_root_sha256,
        "context_id": context_id,
        "task_id": "cli-task-1",
        "task_revision": 1,
        "summary_sha256": preview.summary_sha256,
        "registry_revision": reg_rev,
        "tool_id": "jev-api",
        "model_id": "jev-api:jev-latest",
        "requested_model": "jev-latest",
        "state": {
            "status": "completed",
            "attempt_id": "att-test-1",
            "completed_at_ms": now_ms.saturating_sub(1500),
            "advice": {
                "provider": "jev",
                "model": "jev-1.14.0",
                "difficulty": "easy",
                "confidence": 0.93,
                "needs_review": false,
                "reason_code": "classified",
                "input_tokens": 88,
                "output_tokens": 7
            }
        }
    });
    fs::write(
        &record_path,
        serde_json::to_vec_pretty(&completed_record_json).expect("serialize record"),
    )
    .expect("write completed record");
    fs::set_permissions(&record_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 record");

    let classify_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--cache-ttl-seconds")
        .arg("3600")
        .arg("classify")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run classify cache hit subprocess");
    assert!(
        classify_cmd.status.success(),
        "classify cache hit failed: {}",
        String::from_utf8_lossy(&classify_cmd.stderr)
    );
    let classify_stdout = String::from_utf8_lossy(&classify_cmd.stdout);
    assert!(
        !classify_stdout.contains(raw_summary),
        "classify output must never contain raw task_summary"
    );
    assert!(
        !classify_stdout.contains("profile_id"),
        "classify output without --profile-id must omit profile_id"
    );
    let classify_out: AdviceClassifyOutput =
        serde_json::from_slice(&classify_cmd.stdout).expect("parse classify output");
    assert_eq!(classify_out.profile_id, None);
    assert_eq!(classify_out.source, AdviceSource::Cache);
    assert!(!classify_out.provider_call_performed);
    assert!(classify_out.cache_age_ms.is_some());
    assert_eq!(classify_out.advice.difficulty, Some(Difficulty::Easy));
    assert_eq!(classify_out.agrees_with_baseline, Some(true));
    assert_eq!(classify_out.advice.input_tokens, Some(88));
    assert_eq!(classify_out.advice.output_tokens, Some(7));
}

#[test]
fn cli_profile_id_flag_validation_cache_scoping_and_refusals_work_end_to_end() {
    let fixture = CliTestFixture::new("profile-id-subprocess");
    let mut reg_rev = fixture.seed_jev_registry();
    let store = RegistryStore::open(fixture.registry_dir.clone()).expect("open registry");

    let snap = store
        .apply(
            reg_rev,
            RegistryChange::UpsertModels {
                models: vec![
                    ModelDefinition {
                        id: "jev-api:jev-latest".to_string(),
                        tool_id: "jev-api".to_string(),
                        label: "jev-latest".to_string(),
                        model_arg: "jev-latest".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "jev-api:jev-1.14.0".to_string(),
                        tool_id: "jev-api".to_string(),
                        label: "jev-1.14.0".to_string(),
                        model_arg: "jev-1.14.0".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                ],
            },
        )
        .expect("upsert jev models");
    reg_rev = snap.revision;

    let snap = store
        .apply(
            reg_rev,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "openai-decisions".to_string(),
                    label: "OpenAI Decisions".to_string(),
                    adapter: ToolAdapter::OpenaiDecisionsApi,
                    executable_path: None,
                    enabled: true,
                },
            },
        )
        .expect("upsert openai tool");
    reg_rev = snap.revision;

    let snap = store
        .apply(
            reg_rev,
            RegistryChange::UpsertModels {
                models: vec![
                    ModelDefinition {
                        id: "openai-decisions:gpt-6-luna".to_string(),
                        tool_id: "openai-decisions".to_string(),
                        label: "gpt-6-luna".to_string(),
                        model_arg: "gpt-6-luna".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "openai-decisions:gpt-5-nano".to_string(),
                        tool_id: "openai-decisions".to_string(),
                        label: "gpt-5-nano".to_string(),
                        model_arg: "gpt-5-nano".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                ],
            },
        )
        .expect("upsert openai models");
    reg_rev = snap.revision;

    for (id, advisor_model) in [
        ("profile-jev", Some("jev-api:jev-latest")),
        ("profile-jev-b", Some("jev-api:jev-latest")),
        ("profile-off", None),
        ("profile-other-model", Some("jev-api:jev-1.14.0")),
        ("profile-openai", Some("openai-decisions:gpt-6-luna")),
        ("profile-openai-b", Some("openai-decisions:gpt-6-luna")),
        (
            "profile-openai-unsupported",
            Some("openai-decisions:gpt-5-nano"),
        ),
    ] {
        let snap = store
            .apply(
                reg_rev,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: id.to_string(),
                        name: format!("Profile {id}"),
                        assignments: Vec::new(),
                        decision_advisor: advisor_model.map(|model_id| DecisionAdvisor {
                            model_id: model_id.to_string(),
                            mode: DecisionAdvisorMode::Shadow,
                        }),
                    },
                },
            )
            .expect("upsert profile");
        reg_rev = snap.revision;
    }

    let context_id = "550e8400-e29b-41d4-a716-446655440000";
    let raw_summary = "Profile-scoped CLI summary that must remain redacted in classify output.";
    let req_path = fixture.write_request(
        "request-jev.json",
        &format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "jev-api",
                "model_id": "jev-api:jev-latest",
                "task_id": "cli-prof-task-1",
                "task_revision": 1,
                "task_summary": "{raw_summary}",
                "baseline_difficulty": "easy"
            }}"#
        ),
    );
    let openai_req_path = fixture.write_request(
        "request-openai.json",
        &format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "openai-decisions",
                "model_id": "openai-decisions:gpt-6-luna",
                "task_id": "cli-prof-task-1",
                "task_revision": 1,
                "task_summary": "{raw_summary}",
                "baseline_difficulty": "easy"
            }}"#
        ),
    );
    let openai_unsupported_req_path = fixture.write_request(
        "request-openai-unsupported.json",
        &format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {reg_rev},
                "tool_id": "openai-decisions",
                "model_id": "openai-decisions:gpt-5-nano",
                "task_id": "cli-prof-task-1",
                "task_revision": 1,
                "task_summary": "{raw_summary}",
                "baseline_difficulty": "easy"
            }}"#
        ),
    );

    // 1. Baseline preview without --profile-id vs with --profile-id (before and after subcommand)
    //    for both Jev and OpenAI Decisions profiles.
    let preview_none_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run preview without profile-id");
    assert!(preview_none_cmd.status.success());
    let preview_none: AdvicePreviewOutput =
        serde_json::from_slice(&preview_none_cmd.stdout).expect("parse preview_none");
    assert_eq!(preview_none.profile_id, None);

    let preview_prof_before_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-jev")
        .arg("preview")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run preview with global --profile-id before subcommand");
    assert!(
        preview_prof_before_cmd.status.success(),
        "preview with --profile-id failed: {}",
        String::from_utf8_lossy(&preview_prof_before_cmd.stderr)
    );
    let preview_prof_stdout = String::from_utf8_lossy(&preview_prof_before_cmd.stdout);
    assert!(preview_prof_stdout.contains(r#""profile_id": "profile-jev""#));
    let preview_prof: AdvicePreviewOutput =
        serde_json::from_slice(&preview_prof_before_cmd.stdout).expect("parse preview_prof");
    assert_eq!(preview_prof.profile_id.as_deref(), Some("profile-jev"));
    assert_eq!(preview_prof.provider, DecisionProvider::Jev);
    assert_ne!(preview_prof.cache_key, preview_none.cache_key);

    let preview_prof_after_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--profile-id")
        .arg("profile-jev")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run preview with --profile-id after subcommand");
    assert!(preview_prof_after_cmd.status.success());
    let preview_prof_after: AdvicePreviewOutput =
        serde_json::from_slice(&preview_prof_after_cmd.stdout).expect("parse preview_prof_after");
    assert_eq!(preview_prof_after.cache_key, preview_prof.cache_key);

    let preview_prof_b_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-jev-b")
        .arg("preview")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run preview with profile-jev-b");
    assert!(preview_prof_b_cmd.status.success());
    let preview_prof_b: AdvicePreviewOutput =
        serde_json::from_slice(&preview_prof_b_cmd.stdout).expect("parse preview_prof_b");
    assert_eq!(preview_prof_b.profile_id.as_deref(), Some("profile-jev-b"));
    assert_ne!(preview_prof_b.cache_key, preview_prof.cache_key);
    assert_ne!(preview_prof_b.cache_key, preview_none.cache_key);

    let preview_openai_none_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--request")
        .arg(&openai_req_path)
        .output()
        .expect("run openai preview without profile-id");
    assert!(preview_openai_none_cmd.status.success());
    let preview_openai_none: AdvicePreviewOutput =
        serde_json::from_slice(&preview_openai_none_cmd.stdout).expect("parse openai none");
    assert_eq!(preview_openai_none.profile_id, None);
    assert_eq!(preview_openai_none.provider, DecisionProvider::Openai);

    let preview_openai_prof_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-openai")
        .arg("preview")
        .arg("--request")
        .arg(&openai_req_path)
        .output()
        .expect("run preview with --profile-id profile-openai");
    assert!(
        preview_openai_prof_cmd.status.success(),
        "openai profile preview failed: {}",
        String::from_utf8_lossy(&preview_openai_prof_cmd.stderr)
    );
    let preview_openai_prof_stdout = String::from_utf8_lossy(&preview_openai_prof_cmd.stdout);
    assert!(preview_openai_prof_stdout.contains(r#""profile_id": "profile-openai""#));
    assert!(preview_openai_prof_stdout.contains(r#""provider": "openai""#));
    let preview_openai_prof: AdvicePreviewOutput =
        serde_json::from_slice(&preview_openai_prof_cmd.stdout).expect("parse openai prof");
    assert_eq!(
        preview_openai_prof.profile_id.as_deref(),
        Some("profile-openai")
    );
    assert_eq!(preview_openai_prof.provider, DecisionProvider::Openai);
    assert_eq!(preview_openai_prof.requested_model, "gpt-6-luna");
    assert_ne!(preview_openai_prof.cache_key, preview_openai_none.cache_key);
    assert_ne!(preview_openai_prof.cache_key, preview_prof.cache_key);

    let preview_openai_prof_b_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--profile-id")
        .arg("profile-openai-b")
        .arg("--request")
        .arg(&openai_req_path)
        .output()
        .expect("run preview with --profile-id profile-openai-b");
    assert!(preview_openai_prof_b_cmd.status.success());
    let preview_openai_prof_b: AdvicePreviewOutput =
        serde_json::from_slice(&preview_openai_prof_b_cmd.stdout).expect("parse openai prof b");
    assert_eq!(
        preview_openai_prof_b.profile_id.as_deref(),
        Some("profile-openai-b")
    );
    assert_eq!(preview_openai_prof_b.provider, DecisionProvider::Openai);
    assert_ne!(
        preview_openai_prof_b.cache_key,
        preview_openai_prof.cache_key
    );
    assert_ne!(
        preview_openai_prof_b.cache_key,
        preview_openai_none.cache_key
    );
    assert!(!fixture.cache_dir.exists());

    // 2. Duplicate --profile-id flag is refused with sanitized invalid_input before IO.
    let secret_prof = "sk-live-secret-profile-dup-777";
    let dup_prof_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-jev")
        .arg("--profile-id")
        .arg(secret_prof)
        .arg("preview")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run duplicate --profile-id");
    assert!(!dup_prof_cmd.status.success());
    let dup_prof_stderr = String::from_utf8_lossy(&dup_prof_cmd.stderr);
    assert!(!dup_prof_stderr.contains(secret_prof));
    let dup_prof_err: AdviceError =
        serde_json::from_slice(&dup_prof_cmd.stderr).expect("parse dup_prof_err");
    assert_eq!(dup_prof_err.code(), "invalid_input");

    // 3. `models` with --profile-id is refused before credential or network access.
    for profile_id in ["profile-jev", "profile-openai"] {
        let models_prof_cmd = Command::new(advice_bin())
            .arg("--registry-dir")
            .arg(&fixture.registry_dir)
            .arg("--cache-dir")
            .arg(&fixture.cache_dir)
            .arg("--project-root")
            .arg(&fixture.project_root)
            .arg("--context-id")
            .arg(context_id)
            .arg("--profile-id")
            .arg(profile_id)
            .arg("models")
            .arg("--tool-id")
            .arg("jev-api")
            .output()
            .expect("run models with --profile-id");
        assert!(!models_prof_cmd.status.success());
        let models_prof_err: AdviceError =
            serde_json::from_slice(&models_prof_cmd.stderr).expect("parse models_prof_err");
        assert_eq!(models_prof_err.code(), "invalid_input");
        assert!(models_prof_err.message().contains("--profile-id"));
        assert!(!fixture.cache_dir.exists());
    }

    // 4. Missing profile, off advisor, mismatched model/provider, and unsupported OpenAI model
    //    fail closed before cache_dir creation.
    for (profile_id, req, expected_code) in [
        ("missing-profile", &req_path, "profile_not_found"),
        ("profile-off", &req_path, "decision_advisor_not_configured"),
        (
            "profile-off",
            &openai_req_path,
            "decision_advisor_not_configured",
        ),
        ("profile-other-model", &req_path, "profile_advisor_mismatch"),
        ("profile-openai", &req_path, "profile_advisor_mismatch"),
        ("profile-jev", &openai_req_path, "profile_advisor_mismatch"),
        (
            "profile-openai-unsupported",
            &openai_unsupported_req_path,
            "invalid_model",
        ),
        ("profile-openai-unsupported", &req_path, "invalid_model"),
    ] {
        for subcmd in ["preview", "classify"] {
            let out = Command::new(advice_bin())
                .arg("--registry-dir")
                .arg(&fixture.registry_dir)
                .arg("--cache-dir")
                .arg(&fixture.cache_dir)
                .arg("--project-root")
                .arg(&fixture.project_root)
                .arg("--context-id")
                .arg(context_id)
                .arg("--profile-id")
                .arg(profile_id)
                .arg(subcmd)
                .arg("--request")
                .arg(req)
                .output()
                .expect("run refusal subprocess");
            assert!(
                !out.status.success(),
                "{subcmd} for {profile_id} unexpectedly succeeded"
            );
            let err: AdviceError = serde_json::from_slice(&out.stderr).expect("parse refusal err");
            assert_eq!(
                err.code(),
                expected_code,
                "unexpected code for {subcmd} {profile_id}: {}",
                err.message()
            );
            assert!(
                !fixture.cache_dir.exists(),
                "cache_dir must not be created when {subcmd} fails for {profile_id}"
            );
        }
    }

    // 5. Seed Completed cache records for both profile-jev and profile-openai and verify
    //    classify cache hits + scope checks via real subprocesses without vault or network access.
    fs::create_dir_all(&fixture.cache_dir).expect("create cache_dir");
    fs::set_permissions(&fixture.cache_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 cache_dir");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("unix epoch")
        .as_millis() as u64;
    let project_root_sha256 =
        threadlane_decisions::sha256_hex(preview_prof.project_root.as_bytes());
    let prompt_digest_hex = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        for field in [
            "threadlane-jev-shadow-v1",
            "difficulty",
            "Select implementation difficulty from the supplied task evidence.",
            "Small isolated behavior-preserving edit.",
            "Bounded multi-step change within known module boundaries.",
            "Substantial uncertainty, concurrency, or cross-module integration complexity.",
            "High-risk architectural, security-critical, or multi-system redesign.",
        ] {
            h.update((field.len() as u64).to_le_bytes());
            h.update(field.as_bytes());
        }
        let digest = h.finalize();
        let mut s = String::with_capacity(64);
        for b in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut s, "{b:02x}");
        }
        s
    };
    let openai_prompt_digest_hex = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        for field in [
            "threadlane-openai-shadow-v1",
            "openai",
            "difficulty",
            "Select implementation difficulty from the supplied evidence.",
            "Small isolated behavior-preserving edit.",
            "Bounded multi-step change within known module boundaries.",
            "Substantial uncertainty, concurrency, or cross-module integration complexity.",
            "High-risk architectural, security-critical, or multi-system redesign.",
        ] {
            h.update((field.len() as u64).to_le_bytes());
            h.update(field.as_bytes());
        }
        let digest = h.finalize();
        let mut s = String::with_capacity(64);
        for b in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut s, "{b:02x}");
        }
        s
    };

    let record_path = fixture
        .cache_dir
        .join(format!("{}.json", preview_prof.cache_key));
    let completed_record_json = serde_json::json!({
        "schema_version": ADVICE_CACHE_SCHEMA_VERSION,
        "prompt_digest": prompt_digest_hex,
        "cache_key": preview_prof.cache_key,
        "project_root_sha256": project_root_sha256,
        "context_id": context_id,
        "profile_id": "profile-jev",
        "task_id": "cli-prof-task-1",
        "task_revision": 1,
        "summary_sha256": preview_prof.summary_sha256,
        "registry_revision": reg_rev,
        "tool_id": "jev-api",
        "model_id": "jev-api:jev-latest",
        "requested_model": "jev-latest",
        "state": {
            "status": "completed",
            "attempt_id": "att-prof-1",
            "completed_at_ms": now_ms.saturating_sub(1000),
            "advice": {
                "provider": "jev",
                "model": "jev-1.14.0",
                "difficulty": "easy",
                "confidence": 0.96,
                "needs_review": false,
                "reason_code": "classified",
                "input_tokens": 64,
                "output_tokens": 5
            }
        }
    });
    fs::write(
        &record_path,
        serde_json::to_vec_pretty(&completed_record_json).expect("serialize record"),
    )
    .expect("write completed record");
    fs::set_permissions(&record_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 record");

    let classify_prof_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-jev")
        .arg("--cache-ttl-seconds")
        .arg("3600")
        .arg("classify")
        .arg("--request")
        .arg(&req_path)
        .output()
        .expect("run classify cache hit with --profile-id");
    assert!(
        classify_prof_cmd.status.success(),
        "classify cache hit with --profile-id failed: {}",
        String::from_utf8_lossy(&classify_prof_cmd.stderr)
    );
    let classify_prof_stdout = String::from_utf8_lossy(&classify_prof_cmd.stdout);
    assert!(!classify_prof_stdout.contains(raw_summary));
    assert!(classify_prof_stdout.contains(r#""profile_id": "profile-jev""#));
    let classify_prof_out: AdviceClassifyOutput =
        serde_json::from_slice(&classify_prof_cmd.stdout).expect("parse classify_prof_out");
    assert_eq!(classify_prof_out.profile_id.as_deref(), Some("profile-jev"));
    assert_eq!(classify_prof_out.source, AdviceSource::Cache);
    assert!(!classify_prof_out.provider_call_performed);

    let openai_record_path = fixture
        .cache_dir
        .join(format!("{}.json", preview_openai_prof.cache_key));
    let openai_completed_record_json = serde_json::json!({
        "schema_version": ADVICE_CACHE_SCHEMA_VERSION,
        "prompt_digest": openai_prompt_digest_hex,
        "cache_key": preview_openai_prof.cache_key,
        "project_root_sha256": project_root_sha256,
        "context_id": context_id,
        "profile_id": "profile-openai",
        "provider": "openai",
        "task_id": "cli-prof-task-1",
        "task_revision": 1,
        "summary_sha256": preview_openai_prof.summary_sha256,
        "registry_revision": reg_rev,
        "tool_id": "openai-decisions",
        "model_id": "openai-decisions:gpt-6-luna",
        "requested_model": "gpt-6-luna",
        "state": {
            "status": "completed",
            "attempt_id": "att-prof-openai-1",
            "completed_at_ms": now_ms.saturating_sub(1000),
            "advice": {
                "provider": "openai",
                "model": "gpt-6-luna-2026-10-01",
                "difficulty": "medium",
                "confidence": 0.94,
                "needs_review": false,
                "reason_code": "classified",
                "input_tokens": 72,
                "output_tokens": 0
            }
        }
    });
    fs::write(
        &openai_record_path,
        serde_json::to_vec_pretty(&openai_completed_record_json).expect("serialize openai record"),
    )
    .expect("write completed openai record");
    fs::set_permissions(&openai_record_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 openai record");

    let classify_openai_prof_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-openai")
        .arg("--cache-ttl-seconds")
        .arg("3600")
        .arg("classify")
        .arg("--request")
        .arg(&openai_req_path)
        .output()
        .expect("run openai profile classify cache hit");
    assert!(
        classify_openai_prof_cmd.status.success(),
        "openai profile classify cache hit failed: {}",
        String::from_utf8_lossy(&classify_openai_prof_cmd.stderr)
    );
    let classify_openai_prof_stdout = String::from_utf8_lossy(&classify_openai_prof_cmd.stdout);
    assert!(!classify_openai_prof_stdout.contains(raw_summary));
    assert!(classify_openai_prof_stdout.contains(r#""profile_id": "profile-openai""#));
    let classify_openai_prof_out: AdviceClassifyOutput =
        serde_json::from_slice(&classify_openai_prof_cmd.stdout)
            .expect("parse classify_openai_prof_out");
    assert_eq!(
        classify_openai_prof_out.profile_id.as_deref(),
        Some("profile-openai")
    );
    assert_eq!(classify_openai_prof_out.source, AdviceSource::Cache);
    assert!(!classify_openai_prof_out.provider_call_performed);
    assert_eq!(
        classify_openai_prof_out.advice.provider,
        DecisionProvider::Openai
    );
    assert_eq!(
        classify_openai_prof_out.advice.difficulty,
        Some(Difficulty::Medium)
    );
    assert_eq!(classify_openai_prof_out.agrees_with_baseline, Some(false));
}

#[derive(Default)]
struct CliMockVault {
    secrets: Mutex<HashMap<String, String>>,
    reads: Mutex<Vec<String>>,
    fail_reads: Mutex<bool>,
}

impl CliMockVault {
    fn with_secrets(pairs: &[(&str, &str)]) -> Arc<Self> {
        let mut map = HashMap::new();
        for (tool_id, secret) in pairs {
            map.insert((*tool_id).to_string(), (*secret).to_string());
        }
        Arc::new(Self {
            secrets: Mutex::new(map),
            reads: Mutex::new(Vec::new()),
            fail_reads: Mutex::new(false),
        })
    }

    fn failing() -> Arc<Self> {
        Arc::new(Self {
            secrets: Mutex::new(HashMap::new()),
            reads: Mutex::new(Vec::new()),
            fail_reads: Mutex::new(true),
        })
    }

    fn recorded_reads(&self) -> Vec<String> {
        self.reads.lock().expect("reads lock").clone()
    }
}

impl CredentialStore for CliMockVault {
    fn set(&self, tool_id: &str, secret: &str) -> Result<(), ConnectorError> {
        self.secrets
            .lock()
            .expect("vault lock")
            .insert(tool_id.to_string(), secret.to_string());
        Ok(())
    }

    fn get(&self, tool_id: &str) -> Result<Option<String>, ConnectorError> {
        self.reads
            .lock()
            .expect("reads lock")
            .push(tool_id.to_string());
        if *self.fail_reads.lock().expect("fail_reads lock") {
            return Err(ConnectorError::CredentialStoreUnavailable(
                "OS credential store is unavailable.",
            ));
        }
        Ok(self
            .secrets
            .lock()
            .expect("vault lock")
            .get(tool_id)
            .cloned())
    }

    fn delete(&self, tool_id: &str) -> Result<(), ConnectorError> {
        self.secrets.lock().expect("vault lock").remove(tool_id);
        Ok(())
    }

    fn status(&self, tool_id: &str) -> Result<String, ConnectorError> {
        if *self.fail_reads.lock().expect("fail_reads lock") {
            return Err(ConnectorError::CredentialStoreUnavailable(
                "OS credential store is unavailable.",
            ));
        }
        let configured = self
            .secrets
            .lock()
            .expect("vault lock")
            .contains_key(tool_id);
        Ok(if configured {
            "configured".to_string()
        } else {
            "not_configured".to_string()
        })
    }
}

#[derive(Default)]
struct CliMockTransport {
    requests: Mutex<Vec<TransportRequest>>,
    responses: Mutex<VecDeque<Result<TransportResponse, DecisionError>>>,
}

impl CliMockTransport {
    fn with_responses(
        responses: impl IntoIterator<Item = Result<TransportResponse, DecisionError>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into_iter().collect()),
        })
    }

    fn recorded_requests(&self) -> Vec<TransportRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

impl DecisionTransport for CliMockTransport {
    fn execute<'a>(&'a self, request: TransportRequest) -> TransportFuture<'a> {
        Box::pin(async move {
            self.requests.lock().expect("requests lock").push(request);
            self.responses
                .lock()
                .expect("responses lock")
                .pop_front()
                .unwrap_or(Err(DecisionError::TransportFailed))
        })
    }
}

fn seed_multi_provider_registry(fixture: &CliTestFixture) -> u64 {
    let mut rev = fixture.seed_jev_registry();
    let store = RegistryStore::open(fixture.registry_dir.clone()).expect("open registry");

    for tool in [
        ToolDefinition {
            id: "openai-decisions".to_string(),
            label: "OpenAI Decisions".to_string(),
            adapter: ToolAdapter::OpenaiDecisionsApi,
            executable_path: None,
            enabled: true,
        },
        ToolDefinition {
            id: "openai-disabled".to_string(),
            label: "Disabled OpenAI Decisions".to_string(),
            adapter: ToolAdapter::OpenaiDecisionsApi,
            executable_path: None,
            enabled: false,
        },
        ToolDefinition {
            id: "codex-cli".to_string(),
            label: "Codex CLI".to_string(),
            adapter: ToolAdapter::CodexCli,
            executable_path: Some(PathBuf::from("/usr/local/bin/codex")),
            enabled: true,
        },
    ] {
        let snap = store
            .apply(rev, RegistryChange::UpsertTool { tool })
            .expect("upsert tool");
        rev = snap.revision;
    }

    let snap = store
        .apply(
            rev,
            RegistryChange::UpsertModels {
                models: vec![
                    ModelDefinition {
                        id: "jev-api:gpt-6-luna".to_string(),
                        tool_id: "jev-api".to_string(),
                        label: "mismatched-openai-under-jev".to_string(),
                        model_arg: "gpt-6-luna".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "openai-decisions:gpt-6-luna".to_string(),
                        tool_id: "openai-decisions".to_string(),
                        label: "gpt-6-luna".to_string(),
                        model_arg: "gpt-6-luna".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "openai-decisions:gpt-6-luna-2026-10-01".to_string(),
                        tool_id: "openai-decisions".to_string(),
                        label: "gpt-6-luna-2026-10-01".to_string(),
                        model_arg: "gpt-6-luna-2026-10-01".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "openai-decisions:gpt-5-nano".to_string(),
                        tool_id: "openai-decisions".to_string(),
                        label: "unsupported-openai-model".to_string(),
                        model_arg: "gpt-5-nano".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "openai-decisions:jev-latest".to_string(),
                        tool_id: "openai-decisions".to_string(),
                        label: "mismatched-jev-under-openai".to_string(),
                        model_arg: "jev-latest".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "openai-disabled:gpt-6-luna".to_string(),
                        tool_id: "openai-disabled".to_string(),
                        label: "gpt-6-luna".to_string(),
                        model_arg: "gpt-6-luna".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                    ModelDefinition {
                        id: "codex-cli:gpt-6-luna".to_string(),
                        tool_id: "codex-cli".to_string(),
                        label: "codex-model".to_string(),
                        model_arg: "gpt-6-luna".to_string(),
                        efforts: Vec::new(),
                        allow_no_effort: true,
                    },
                ],
            },
        )
        .expect("upsert models");
    rev = snap.revision;

    for (id, model_id) in [
        ("profile-jev", "jev-api:jev-latest"),
        ("profile-openai", "openai-decisions:gpt-6-luna"),
    ] {
        let snap = store
            .apply(
                rev,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: id.to_string(),
                        name: format!("Profile {id}"),
                        assignments: Vec::new(),
                        decision_advisor: Some(DecisionAdvisor {
                            model_id: model_id.to_string(),
                            mode: DecisionAdvisorMode::Shadow,
                        }),
                    },
                },
            )
            .expect("upsert profile");
        rev = snap.revision;
    }

    rev
}

#[test]
fn cli_openai_preview_profile_routing_refusals_and_jev_scoped_models_work_offline() {
    let fixture = CliTestFixture::new("openai-preview-refusals");
    let reg_rev = seed_multi_provider_registry(&fixture);
    let context_id = "550e8400-e29b-41d4-a716-446655440010";
    let raw_summary = "OpenAI Decisions offline preview task summary.";

    let write_req = |name: &str, tool_id: &str, model_id: &str| -> PathBuf {
        fixture.write_request(
            name,
            &format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "{tool_id}",
                    "model_id": "{model_id}",
                    "task_id": "openai-cli-task-1",
                    "task_revision": 1,
                    "task_summary": "{raw_summary}",
                    "baseline_difficulty": "medium"
                }}"#
            ),
        )
    };

    let openai_req = write_req(
        "req-openai.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna",
    );
    let openai_pinned_req = write_req(
        "req-openai-pinned.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna-2026-10-01",
    );
    let jev_req = write_req("req-jev.json", "jev-api", "jev-api:jev-latest");

    // 1. OpenAI Decisions `preview` succeeds offline, resolves `"openai"`, omits `profile_id`,
    //    and does not create `cache_dir`.
    let preview_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--request")
        .arg(&openai_req)
        .output()
        .expect("run openai preview");
    assert!(
        preview_cmd.status.success(),
        "openai preview failed: {}",
        String::from_utf8_lossy(&preview_cmd.stderr)
    );
    assert!(!fixture.cache_dir.exists());
    let preview_stdout = String::from_utf8_lossy(&preview_cmd.stdout);
    assert!(!preview_stdout.contains("profile_id"));
    assert!(preview_stdout.contains(r#""provider": "openai""#));
    let openai_preview: AdvicePreviewOutput =
        serde_json::from_slice(&preview_cmd.stdout).expect("parse openai preview");
    assert!(openai_preview.untrusted_data);
    assert!(openai_preview.advisory_only);
    assert_eq!(openai_preview.mode, "shadow");
    assert_eq!(openai_preview.provider, DecisionProvider::Openai);
    assert_eq!(openai_preview.tool_id, "openai-decisions");
    assert_eq!(openai_preview.model_id, "openai-decisions:gpt-6-luna");
    assert_eq!(openai_preview.requested_model, "gpt-6-luna");
    assert_eq!(openai_preview.task_summary, raw_summary);
    assert_eq!(openai_preview.baseline_difficulty, Some(Difficulty::Medium));
    assert!(!openai_preview.provider_call_performed);

    // 2. Versioned OpenAI model preview and Jev preview on the exact same task/summary
    //    produce distinct provider-scoped cache keys.
    let pinned_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--request")
        .arg(&openai_pinned_req)
        .output()
        .expect("run pinned openai preview");
    assert!(pinned_cmd.status.success());
    let pinned_preview: AdvicePreviewOutput =
        serde_json::from_slice(&pinned_cmd.stdout).expect("parse pinned openai preview");
    assert_eq!(pinned_preview.provider, DecisionProvider::Openai);
    assert_eq!(pinned_preview.requested_model, "gpt-6-luna-2026-10-01");
    assert_ne!(pinned_preview.cache_key, openai_preview.cache_key);

    let jev_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("preview")
        .arg("--request")
        .arg(&jev_req)
        .output()
        .expect("run jev preview");
    assert!(jev_cmd.status.success());
    let jev_preview: AdvicePreviewOutput =
        serde_json::from_slice(&jev_cmd.stdout).expect("parse jev preview");
    assert_eq!(jev_preview.provider, DecisionProvider::Jev);
    assert_ne!(jev_preview.cache_key, openai_preview.cache_key);

    // 3. Refusals for disabled OpenAI tool, mismatched tool/model pairs, invalid model_arg,
    //    and non-decision adapters fail closed on both `preview` and `classify` before creating cache_dir.
    let refusal_cases = [
        (
            "disabled-openai",
            write_req(
                "req-disabled-openai.json",
                "openai-disabled",
                "openai-disabled:gpt-6-luna",
            ),
            "tool_disabled",
        ),
        (
            "openai-tool-with-jev-model",
            write_req(
                "req-cross-tool-1.json",
                "openai-decisions",
                "jev-api:jev-latest",
            ),
            "model_not_found",
        ),
        (
            "jev-tool-with-openai-model",
            write_req(
                "req-cross-tool-2.json",
                "jev-api",
                "openai-decisions:gpt-6-luna",
            ),
            "model_not_found",
        ),
        (
            "openai-tool-with-unsupported-model-arg",
            write_req(
                "req-unsupported-openai-model.json",
                "openai-decisions",
                "openai-decisions:gpt-5-nano",
            ),
            "invalid_model",
        ),
        (
            "openai-tool-with-jev-model-arg",
            write_req(
                "req-jev-arg-under-openai.json",
                "openai-decisions",
                "openai-decisions:jev-latest",
            ),
            "invalid_model",
        ),
        (
            "jev-tool-with-openai-model-arg",
            write_req(
                "req-openai-arg-under-jev.json",
                "jev-api",
                "jev-api:gpt-6-luna",
            ),
            "invalid_model",
        ),
        (
            "codex-cli-adapter",
            write_req("req-codex-cli.json", "codex-cli", "codex-cli:gpt-6-luna"),
            "unsupported_adapter",
        ),
    ];

    for (label, req_path, expected_code) in &refusal_cases {
        for subcmd in ["preview", "classify"] {
            let out = Command::new(advice_bin())
                .arg("--registry-dir")
                .arg(&fixture.registry_dir)
                .arg("--cache-dir")
                .arg(&fixture.cache_dir)
                .arg("--project-root")
                .arg(&fixture.project_root)
                .arg("--context-id")
                .arg(context_id)
                .arg(subcmd)
                .arg("--request")
                .arg(req_path)
                .output()
                .expect("run refusal subprocess");
            assert!(
                !out.status.success(),
                "{subcmd} for {label} unexpectedly succeeded"
            );
            let err: AdviceError = serde_json::from_slice(&out.stderr).expect("parse error json");
            assert_eq!(
                err.code(),
                *expected_code,
                "unexpected error code for {subcmd} {label}: {}",
                err.message()
            );
            assert!(
                !fixture.cache_dir.exists(),
                "cache_dir must not be created when {subcmd} fails for {label}"
            );
        }
    }

    // 4. `models` stays scoped to Jev (`unsupported_adapter` for OpenAI), while `--profile-id`
    //    routes by the saved advisor's enabled owning adapter (Jev or OpenAI Decisions) and
    //    enforces exact tool/model/revision matching before cache_dir creation.
    let models_openai_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("models")
        .arg("--tool-id")
        .arg("openai-decisions")
        .output()
        .expect("run models for openai-decisions");
    assert!(!models_openai_cmd.status.success());
    let models_err: AdviceError =
        serde_json::from_slice(&models_openai_cmd.stderr).expect("parse models error");
    assert_eq!(models_err.code(), "unsupported_adapter");
    assert!(!fixture.cache_dir.exists());

    let profile_openai_preview_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-openai")
        .arg("preview")
        .arg("--request")
        .arg(&openai_req)
        .output()
        .expect("run profile-openai preview");
    assert!(
        profile_openai_preview_cmd.status.success(),
        "profile-openai preview failed: {}",
        String::from_utf8_lossy(&profile_openai_preview_cmd.stderr)
    );
    assert!(!fixture.cache_dir.exists());
    let profile_openai_preview: AdvicePreviewOutput =
        serde_json::from_slice(&profile_openai_preview_cmd.stdout)
            .expect("parse profile-openai preview");
    assert_eq!(
        profile_openai_preview.profile_id.as_deref(),
        Some("profile-openai")
    );
    assert_eq!(profile_openai_preview.provider, DecisionProvider::Openai);
    assert_eq!(profile_openai_preview.tool_id, "openai-decisions");
    assert_eq!(
        profile_openai_preview.model_id,
        "openai-decisions:gpt-6-luna"
    );
    assert_eq!(profile_openai_preview.requested_model, "gpt-6-luna");
    assert_ne!(profile_openai_preview.cache_key, openai_preview.cache_key);
    assert_ne!(profile_openai_preview.cache_key, jev_preview.cache_key);

    let disabled_openai_req = write_req(
        "req-prof-disabled-openai.json",
        "openai-disabled",
        "openai-disabled:gpt-6-luna",
    );
    let codex_profile_req = write_req("req-prof-codex.json", "codex-cli", "codex-cli:gpt-6-luna");
    let missing_tool_req = write_req(
        "req-prof-missing-tool.json",
        "missing-openai",
        "missing-openai:gpt-6-luna",
    );
    let stale_rev_openai_req = fixture.write_request(
        "req-prof-stale-rev.json",
        &format!(
            r#"{{
                "schema_version": 1,
                "expected_registry_revision": {},
                "tool_id": "openai-decisions",
                "model_id": "openai-decisions:gpt-6-luna",
                "task_id": "openai-cli-task-1",
                "task_revision": 1,
                "task_summary": "{raw_summary}",
                "baseline_difficulty": "medium"
            }}"#,
            reg_rev + 1
        ),
    );

    for (label, profile_id, req_path, expected_code) in [
        (
            "openai-profile-pinned-model-mismatch",
            "profile-openai",
            &openai_pinned_req,
            "profile_advisor_mismatch",
        ),
        (
            "openai-profile-jev-request-mismatch",
            "profile-openai",
            &jev_req,
            "profile_advisor_mismatch",
        ),
        (
            "jev-profile-openai-request-mismatch",
            "profile-jev",
            &openai_req,
            "profile_advisor_mismatch",
        ),
        (
            "openai-profile-disabled-tool-request",
            "profile-openai",
            &disabled_openai_req,
            "tool_disabled",
        ),
        (
            "openai-profile-codex-adapter-request",
            "profile-openai",
            &codex_profile_req,
            "unsupported_adapter",
        ),
        (
            "openai-profile-missing-tool-request",
            "profile-openai",
            &missing_tool_req,
            "tool_not_found",
        ),
        (
            "openai-profile-stale-revision",
            "profile-openai",
            &stale_rev_openai_req,
            "stale_registry_revision",
        ),
    ] {
        for subcmd in ["preview", "classify"] {
            let out = Command::new(advice_bin())
                .arg("--registry-dir")
                .arg(&fixture.registry_dir)
                .arg("--cache-dir")
                .arg(&fixture.cache_dir)
                .arg("--project-root")
                .arg(&fixture.project_root)
                .arg("--context-id")
                .arg(context_id)
                .arg("--profile-id")
                .arg(profile_id)
                .arg(subcmd)
                .arg("--request")
                .arg(req_path)
                .output()
                .expect("run profile-scoped refusal");
            assert!(
                !out.status.success(),
                "{subcmd} for {label} unexpectedly succeeded"
            );
            let err: AdviceError =
                serde_json::from_slice(&out.stderr).expect("parse profile refusal error");
            assert_eq!(
                err.code(),
                expected_code,
                "unexpected code for {subcmd} {label}: {}",
                err.message()
            );
            assert!(
                !fixture.cache_dir.exists(),
                "cache_dir must not be created when {subcmd} fails for {label}"
            );
        }
    }

    // 5. Busy per-key locks on explicit OpenAI and profile-scoped OpenAI cache keys refuse
    //    immediately via subprocess with `cache_busy` and stay isolated from one another.
    fs::create_dir_all(&fixture.cache_dir).expect("create cache_dir");
    fs::set_permissions(&fixture.cache_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 cache_dir");
    for (profile_arg, cache_key) in [
        (None, openai_preview.cache_key.as_str()),
        (
            Some("profile-openai"),
            profile_openai_preview.cache_key.as_str(),
        ),
    ] {
        let lock_path = fixture.cache_dir.join(format!("{cache_key}.lock"));
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .expect("create openai lock file");
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 openai lock file");
        lock_file
            .try_lock_exclusive()
            .expect("hold exclusive openai lock");

        let mut cmd = Command::new(advice_bin());
        cmd.arg("--registry-dir")
            .arg(&fixture.registry_dir)
            .arg("--cache-dir")
            .arg(&fixture.cache_dir)
            .arg("--project-root")
            .arg(&fixture.project_root)
            .arg("--context-id")
            .arg(context_id);
        if let Some(pid) = profile_arg {
            cmd.arg("--profile-id").arg(pid);
        }
        let busy_cmd = cmd
            .arg("classify")
            .arg("--request")
            .arg(&openai_req)
            .output()
            .expect("run busy openai classify");
        assert!(!busy_cmd.status.success());
        let busy_err: AdviceError =
            serde_json::from_slice(&busy_cmd.stderr).expect("parse busy error json");
        assert_eq!(busy_err.code(), "cache_busy");
        lock_file.unlock().expect("unlock openai lock");
    }
}

#[tokio::test]
async fn cli_openai_classify_mock_transport_caching_no_second_call_and_cross_provider_isolation() {
    let fixture = CliTestFixture::new("openai-mock-transport-e2e");
    let reg_rev = seed_multi_provider_registry(&fixture);
    let context_id = "550e8400-e29b-41d4-a716-446655440011";
    let scope = AdviceScope {
        registry_dir: fixture.registry_dir.clone(),
        cache_dir: fixture.cache_dir.clone(),
        project_root: fixture.project_root.clone(),
        context_id: context_id.to_string(),
        cache_ttl_seconds: DEFAULT_CACHE_TTL_SECONDS,
        profile_id: None,
    };

    let openai_secret = "sk-proj-openai-mock-secret-999";
    let jev_secret = "ts-jev-mock-secret-888";
    let raw_summary = "Classify cross-crate lock state transitions without external calls.";

    let write_req = |name: &str, tool_id: &str, model_id: &str, task_id: &str| -> PathBuf {
        fixture.write_request(
            name,
            &format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "{tool_id}",
                    "model_id": "{model_id}",
                    "task_id": "{task_id}",
                    "task_revision": 1,
                    "task_summary": "{raw_summary}",
                    "baseline_difficulty": "easy"
                }}"#
            ),
        )
    };

    let openai_req_1 = write_req(
        "openai-1.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna",
        "task-shared-1",
    );
    let jev_req_1 = write_req(
        "jev-1.json",
        "jev-api",
        "jev-api:jev-latest",
        "task-shared-1",
    );
    let openai_req_2 = write_req(
        "openai-2.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna",
        "task-shared-2",
    );
    let jev_req_2 = write_req(
        "jev-2.json",
        "jev-api",
        "jev-api:jev-latest",
        "task-shared-2",
    );
    let openai_req_refusal = write_req(
        "openai-refusal.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna",
        "task-refusal-3",
    );

    let openai_only_vault = CliMockVault::with_secrets(&[("openai-decisions", openai_secret)]);
    let transport = CliMockTransport::with_responses([
        Ok(TransportResponse {
            status: 200,
            body: br#"{
                "model": "gpt-6-luna-2026-10-01",
                "answers": [{
                    "type": "choice",
                    "name": "difficulty",
                    "choice": "hard",
                    "confidence": 0.92,
                    "probabilities": [
                        {"value": "hard", "probability": 0.92},
                        {"value": "medium", "probability": 0.08}
                    ]
                }],
                "usage": {
                    "input_tokens": 112,
                    "output_tokens": 0,
                    "total_tokens": 112
                }
            }"#
            .to_vec(),
        }),
        Ok(TransportResponse {
            status: 429,
            body: br#"{"error":"rate_limited"}"#.to_vec(),
        }),
        Ok(TransportResponse {
            status: 200,
            body: br#"{
                "model": "jev-1.14.0",
                "answers": {
                    "difficulty": {
                        "type": "choice",
                        "choice": "medium",
                        "confidence": 0.89,
                        "probabilities": {
                            "easy": 0.04,
                            "medium": 0.89,
                            "hard": 0.05,
                            "extreme": 0.02
                        }
                    }
                },
                "usage": {"input_tokens": 95, "output_tokens": 9}
            }"#
            .to_vec(),
        }),
        Ok(TransportResponse {
            status: 200,
            body: br#"{
                "model": "gpt-6-luna",
                "answers": [{
                    "type": "refusal",
                    "name": "difficulty"
                }],
                "usage": {
                    "input_tokens": 64,
                    "output_tokens": 0,
                    "total_tokens": 64
                }
            }"#
            .to_vec(),
        }),
    ]);

    let openai_only_client =
        DecisionClient::with_transport(openai_only_vault.clone(), transport.clone())
            .expect("init openai-only client");

    // 1. `list_advice_models` stays scoped to Jev and refuses OpenAI before reading credentials.
    let models_err = list_advice_models(&scope, "openai-decisions", &openai_only_client)
        .await
        .expect_err("models must stay scoped to Jev");
    assert_eq!(models_err.code(), "unsupported_adapter");
    assert!(openai_only_vault.recorded_reads().is_empty());
    assert!(transport.recorded_requests().is_empty());

    // 2. First OpenAI Decisions classify calls mock transport targeting OPENAI_CLASSIFY_URL.
    let first = classify_advice(
        &scope,
        &openai_req_1,
        false,
        openai_only_vault.clone(),
        &openai_only_client,
    )
    .await
    .expect("first openai classify should succeed");
    assert!(first.untrusted_data);
    assert!(first.advisory_only);
    assert_eq!(first.mode, "shadow");
    assert_eq!(first.source, AdviceSource::Provider);
    assert!(first.provider_call_performed);
    assert_eq!(first.tool_id, "openai-decisions");
    assert_eq!(first.model_id, "openai-decisions:gpt-6-luna");
    assert_eq!(first.requested_model, "gpt-6-luna");
    assert_eq!(first.advice.provider, DecisionProvider::Openai);
    assert_eq!(first.advice.model, "gpt-6-luna-2026-10-01");
    assert_eq!(first.advice.difficulty, Some(Difficulty::Hard));
    assert_eq!(first.advice.confidence, Some(0.92));
    assert!(!first.advice.needs_review);
    assert_eq!(first.advice.reason_code, "classified");
    assert_eq!(first.advice.input_tokens, Some(112));
    assert_eq!(first.advice.output_tokens, Some(0));
    assert_eq!(first.agrees_with_baseline, Some(false));

    let first_json = serde_json::to_string(&first).expect("serialize first output");
    assert!(!first_json.contains(raw_summary));
    assert!(!first_json.contains(openai_secret));

    let recorded = transport.recorded_requests();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].provider, DecisionProvider::Openai);
    assert_eq!(recorded[0].method, TransportMethod::Post);
    assert_eq!(recorded[0].url, OPENAI_CLASSIFY_URL);
    assert_eq!(recorded[0].bearer_token, openai_secret);
    let sent_body: serde_json::Value =
        serde_json::from_str(recorded[0].json_body.as_deref().expect("openai post body"))
            .expect("parse sent openai json");
    assert_eq!(sent_body["model"], "gpt-6-luna");
    assert_eq!(sent_body["input"], raw_summary);
    assert_eq!(sent_body["questions"][0]["type"], "choice");
    assert_eq!(sent_body["questions"][0]["name"], "difficulty");

    let record_path = fixture.cache_dir.join(format!("{}.json", first.cache_key));
    let persisted_text = fs::read_to_string(&record_path).expect("read openai cache file");
    assert!(!persisted_text.contains(raw_summary));
    assert!(!persisted_text.contains(openai_secret));
    let persisted_val: serde_json::Value =
        serde_json::from_str(&persisted_text).expect("parse openai cache file");
    assert_eq!(persisted_val["provider"], "openai");

    // 3. Second call hits cache with ZERO extra provider calls and ZERO vault reads — both via
    //    `classify_advice` with a failing vault AND via the real `threadlane-advice` CLI subprocess!
    let failing_vault = CliMockVault::failing();
    let failing_client =
        DecisionClient::with_transport(failing_vault.clone(), transport.clone()).expect("client");
    let cached = classify_advice(
        &scope,
        &openai_req_1,
        false,
        failing_vault.clone(),
        &failing_client,
    )
    .await
    .expect("second call must hit cache without vault or network");
    assert_eq!(cached.source, AdviceSource::Cache);
    assert!(!cached.provider_call_performed);
    assert!(cached.cache_age_ms.is_some());
    assert_eq!(cached.advice, first.advice);
    assert!(failing_vault.recorded_reads().is_empty());
    assert_eq!(transport.recorded_requests().len(), 1);

    let cli_hit_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("classify")
        .arg("--request")
        .arg(&openai_req_1)
        .output()
        .expect("run subprocess openai cache hit");
    assert!(
        cli_hit_cmd.status.success(),
        "subprocess openai cache hit failed: {}",
        String::from_utf8_lossy(&cli_hit_cmd.stderr)
    );
    let cli_hit_stdout = String::from_utf8_lossy(&cli_hit_cmd.stdout);
    assert!(!cli_hit_stdout.contains(raw_summary));
    assert!(!cli_hit_stdout.contains(openai_secret));
    let cli_hit: AdviceClassifyOutput =
        serde_json::from_slice(&cli_hit_cmd.stdout).expect("parse subprocess classify output");
    assert_eq!(cli_hit.source, AdviceSource::Cache);
    assert!(!cli_hit.provider_call_performed);
    assert_eq!(cli_hit.advice.provider, DecisionProvider::Openai);
    assert_eq!(cli_hit.advice.difficulty, Some(Difficulty::Hard));

    // 4. Separate credentials and no cross-provider reuse/fallback: Jev request for the exact same
    //    task must NOT reuse OpenAI's cache entry or OpenAI's credential.
    let jev_missing_cred = classify_advice(
        &scope,
        &jev_req_1,
        false,
        openai_only_vault.clone(),
        &openai_only_client,
    )
    .await
    .expect_err("jev without jev credential must fail closed without reusing openai cache/key");
    assert_eq!(jev_missing_cred.code(), "not_configured");
    assert_eq!(transport.recorded_requests().len(), 1);

    // 5. OpenAI rate-limit error makes exactly 1 attempt (no retry), seals `Uncertain`, blocks
    //    subprocess replay without `--refresh`, and does NOT block Jev for the same task!
    let rate_err = classify_advice(
        &scope,
        &openai_req_2,
        false,
        openai_only_vault.clone(),
        &openai_only_client,
    )
    .await
    .expect_err("429 must fail immediately without retry");
    assert_eq!(rate_err.code(), "rate_limited");
    assert_eq!(transport.recorded_requests().len(), 2);

    let cli_uncertain_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("classify")
        .arg("--request")
        .arg(&openai_req_2)
        .output()
        .expect("run subprocess uncertain refusal");
    assert!(!cli_uncertain_cmd.status.success());
    let cli_uncertain_err: AdviceError =
        serde_json::from_slice(&cli_uncertain_cmd.stderr).expect("parse uncertain err");
    assert_eq!(
        cli_uncertain_err.code(),
        "uncertain_attempt_requires_refresh"
    );

    let both_vault =
        CliMockVault::with_secrets(&[("openai-decisions", openai_secret), ("jev-api", jev_secret)]);
    let both_client =
        DecisionClient::with_transport(both_vault.clone(), transport.clone()).expect("client");

    let jev_2_out = classify_advice(&scope, &jev_req_2, false, both_vault.clone(), &both_client)
        .await
        .expect("jev for same task must not be blocked by openai uncertain state");
    assert_eq!(jev_2_out.source, AdviceSource::Provider);
    assert_eq!(jev_2_out.advice.provider, DecisionProvider::Jev);
    assert_eq!(transport.recorded_requests().len(), 3);
    assert_eq!(
        transport.recorded_requests()[2].provider,
        DecisionProvider::Jev
    );
    assert_eq!(transport.recorded_requests()[2].url, JEV_CLASSIFY_URL);

    // 6. OpenAI refusal response is persisted and served offline via both library and CLI subprocess.
    let refusal = classify_advice(
        &scope,
        &openai_req_refusal,
        false,
        both_vault.clone(),
        &both_client,
    )
    .await
    .expect("openai refusal must succeed as advisory output");
    assert_eq!(refusal.source, AdviceSource::Provider);
    assert_eq!(refusal.advice.provider, DecisionProvider::Openai);
    assert_eq!(refusal.advice.difficulty, None);
    assert_eq!(refusal.advice.confidence, None);
    assert!(refusal.advice.needs_review);
    assert_eq!(refusal.advice.reason_code, "refusal");
    assert_eq!(refusal.agrees_with_baseline, None);
    assert_eq!(transport.recorded_requests().len(), 4);

    let cli_refusal_hit = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("classify")
        .arg("--request")
        .arg(&openai_req_refusal)
        .output()
        .expect("run subprocess cached refusal");
    assert!(
        cli_refusal_hit.status.success(),
        "subprocess cached refusal failed: {}",
        String::from_utf8_lossy(&cli_refusal_hit.stderr)
    );
    let cli_refusal_out: AdviceClassifyOutput =
        serde_json::from_slice(&cli_refusal_hit.stdout).expect("parse cached refusal output");
    assert_eq!(cli_refusal_out.source, AdviceSource::Cache);
    assert!(!cli_refusal_out.provider_call_performed);
    assert_eq!(cli_refusal_out.advice.provider, DecisionProvider::Openai);
    assert_eq!(cli_refusal_out.advice.reason_code, "refusal");
    assert_eq!(cli_refusal_out.agrees_with_baseline, None);

    // 7. Cross-provider tampering on OpenAI's persisted cache file is refused by subprocess with `corrupt_cache`.
    let mut tampered = persisted_val;
    tampered["provider"] = serde_json::Value::String("jev".to_string());
    fs::write(
        &record_path,
        serde_json::to_vec_pretty(&tampered).expect("serialize tampered openai record"),
    )
    .expect("write tampered openai record");
    fs::set_permissions(&record_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 tampered openai record");

    let cli_tampered_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("classify")
        .arg("--request")
        .arg(&openai_req_1)
        .output()
        .expect("run subprocess tampered cache refusal");
    assert!(!cli_tampered_cmd.status.success());
    let cli_tampered_err: AdviceError =
        serde_json::from_slice(&cli_tampered_cmd.stderr).expect("parse tampered err");
    assert_eq!(cli_tampered_err.code(), "corrupt_cache");
}

#[tokio::test]
async fn cli_openai_profile_classify_mock_transport_cache_resume_isolation_and_side_effect_free_comparison(
) {
    let fixture = CliTestFixture::new("openai-profile-mock-e2e");
    let mut reg_rev = seed_multi_provider_registry(&fixture);
    let store = RegistryStore::open(fixture.registry_dir.clone()).expect("open registry");

    // Add a second enabled OpenAI Decisions tool, a second OpenAI profile sharing the primary model,
    // a role assignment on profile-openai to verify comparison stays side-effect-free to routing,
    // and set default_profile_id to profile-jev to verify no silent default/fallback selection.
    let snap = store
        .apply(
            reg_rev,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "openai-alt".to_string(),
                    label: "Alternate OpenAI Decisions".to_string(),
                    adapter: ToolAdapter::OpenaiDecisionsApi,
                    executable_path: None,
                    enabled: true,
                },
            },
        )
        .expect("upsert openai-alt tool");
    reg_rev = snap.revision;

    let snap = store
        .apply(
            reg_rev,
            RegistryChange::UpsertModel {
                model: ModelDefinition {
                    id: "openai-alt:gpt-6-luna".to_string(),
                    tool_id: "openai-alt".to_string(),
                    label: "gpt-6-luna".to_string(),
                    model_arg: "gpt-6-luna".to_string(),
                    efforts: Vec::new(),
                    allow_no_effort: true,
                },
            },
        )
        .expect("upsert openai-alt model");
    reg_rev = snap.revision;

    let snap = store
        .apply(
            reg_rev,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    id: "profile-openai".to_string(),
                    name: "Profile OpenAI With Routing".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Easy),
                        choice: ModelChoice {
                            model_id: "codex-cli:gpt-6-luna".to_string(),
                            effort: None,
                        },
                    }],
                    decision_advisor: Some(DecisionAdvisor {
                        model_id: "openai-decisions:gpt-6-luna".to_string(),
                        mode: DecisionAdvisorMode::Shadow,
                    }),
                },
            },
        )
        .expect("update profile-openai with role assignment");
    reg_rev = snap.revision;

    let snap = store
        .apply(
            reg_rev,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    id: "profile-openai-b".to_string(),
                    name: "Profile OpenAI B".to_string(),
                    assignments: Vec::new(),
                    decision_advisor: Some(DecisionAdvisor {
                        model_id: "openai-decisions:gpt-6-luna".to_string(),
                        mode: DecisionAdvisorMode::Shadow,
                    }),
                },
            },
        )
        .expect("upsert profile-openai-b");
    reg_rev = snap.revision;

    let snap = store
        .apply(
            reg_rev,
            RegistryChange::SetDefaultProfile {
                profile_id: Some("profile-jev".to_string()),
            },
        )
        .expect("set default profile to profile-jev");
    reg_rev = snap.revision;

    let registry_file = fixture.registry_dir.join("registry.json");
    let reg_bytes_before = fs::read(&registry_file).expect("read registry bytes before");

    let context_id = "550e8400-e29b-41d4-a716-446655440022";
    let scope_unscoped = AdviceScope {
        registry_dir: fixture.registry_dir.clone(),
        cache_dir: fixture.cache_dir.clone(),
        project_root: fixture.project_root.clone(),
        context_id: context_id.to_string(),
        cache_ttl_seconds: DEFAULT_CACHE_TTL_SECONDS,
        profile_id: None,
    };
    let scope_openai_a = AdviceScope {
        profile_id: Some("profile-openai".to_string()),
        ..scope_unscoped.clone()
    };
    let scope_openai_b = AdviceScope {
        profile_id: Some("profile-openai-b".to_string()),
        ..scope_unscoped.clone()
    };
    let scope_jev = AdviceScope {
        profile_id: Some("profile-jev".to_string()),
        ..scope_unscoped.clone()
    };

    let openai_secret = "sk-proj-openai-profile-secret-111";
    let jev_secret = "ts-jev-profile-secret-222";
    let raw_summary = "Evaluate OpenAI profile advisor routing without touching model routing.";

    let write_req = |name: &str, tool_id: &str, model_id: &str, task_id: &str| -> PathBuf {
        fixture.write_request(
            name,
            &format!(
                r#"{{
                    "schema_version": 1,
                    "expected_registry_revision": {reg_rev},
                    "tool_id": "{tool_id}",
                    "model_id": "{model_id}",
                    "task_id": "{task_id}",
                    "task_revision": 1,
                    "task_summary": "{raw_summary}",
                    "baseline_difficulty": "easy"
                }}"#
            ),
        )
    };

    let req_openai_1 = write_req(
        "prof-openai-1.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna",
        "task-prof-1",
    );
    let req_openai_alt = write_req(
        "prof-openai-alt.json",
        "openai-alt",
        "openai-alt:gpt-6-luna",
        "task-prof-1",
    );
    let req_openai_pinned = write_req(
        "prof-openai-pinned.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna-2026-10-01",
        "task-prof-1",
    );
    let req_jev_1 = write_req(
        "prof-jev-1.json",
        "jev-api",
        "jev-api:jev-latest",
        "task-prof-1",
    );
    let req_openai_2 = write_req(
        "prof-openai-2.json",
        "openai-decisions",
        "openai-decisions:gpt-6-luna",
        "task-prof-2",
    );
    let req_jev_2 = write_req(
        "prof-jev-2.json",
        "jev-api",
        "jev-api:jev-latest",
        "task-prof-2",
    );

    // 1. Offline preview confirms distinct cache keys across unscoped OpenAI, profile-openai,
    //    profile-openai-b, and profile-jev without creating cache_dir.
    let prev_unscoped = preview_advice(&scope_unscoped, &req_openai_1).expect("unscoped preview");
    let prev_openai_a = preview_advice(&scope_openai_a, &req_openai_1).expect("openai a preview");
    let prev_openai_b = preview_advice(&scope_openai_b, &req_openai_1).expect("openai b preview");
    let prev_jev = preview_advice(&scope_jev, &req_jev_1).expect("jev preview");
    assert_eq!(prev_unscoped.profile_id, None);
    assert_eq!(prev_unscoped.provider, DecisionProvider::Openai);
    assert_eq!(prev_openai_a.profile_id.as_deref(), Some("profile-openai"));
    assert_eq!(prev_openai_a.provider, DecisionProvider::Openai);
    assert_eq!(
        prev_openai_b.profile_id.as_deref(),
        Some("profile-openai-b")
    );
    assert_eq!(prev_openai_b.provider, DecisionProvider::Openai);
    assert_eq!(prev_jev.profile_id.as_deref(), Some("profile-jev"));
    assert_eq!(prev_jev.provider, DecisionProvider::Jev);
    assert_ne!(prev_openai_a.cache_key, prev_unscoped.cache_key);
    assert_ne!(prev_openai_a.cache_key, prev_openai_b.cache_key);
    assert_ne!(prev_openai_a.cache_key, prev_jev.cache_key);
    assert!(!fixture.cache_dir.exists());

    // 2. Preflight mismatches (wrong tool, wrong model, cross-provider) fail closed before
    //    vault access, network calls, or cache_dir creation.
    let failing_vault = CliMockVault::failing();
    let transport = CliMockTransport::with_responses([
        Ok(TransportResponse {
            status: 200,
            body: br#"{
                "model": "gpt-6-luna-2026-10-01",
                "answers": [{
                    "type": "choice",
                    "name": "difficulty",
                    "choice": "extreme",
                    "confidence": 0.97,
                    "probabilities": [
                        {"value": "extreme", "probability": 0.97},
                        {"value": "hard", "probability": 0.03}
                    ]
                }],
                "usage": {"input_tokens": 118, "output_tokens": 0, "total_tokens": 118}
            }"#
            .to_vec(),
        }),
        Ok(TransportResponse {
            status: 200,
            body: br#"{
                "model": "gpt-6-luna",
                "answers": [{
                    "type": "refusal",
                    "name": "difficulty"
                }],
                "usage": {"input_tokens": 60, "output_tokens": 0, "total_tokens": 60}
            }"#
            .to_vec(),
        }),
        Ok(TransportResponse {
            status: 200,
            body: br#"{
                "model": "gpt-6-luna",
                "answers": [{
                    "type": "choice",
                    "name": "difficulty",
                    "choice": "medium",
                    "confidence": 0.89,
                    "probabilities": [
                        {"value": "medium", "probability": 0.89},
                        {"value": "easy", "probability": 0.11}
                    ]
                }],
                "usage": {"input_tokens": 105, "output_tokens": 0, "total_tokens": 105}
            }"#
            .to_vec(),
        }),
        Ok(TransportResponse {
            status: 429,
            body: br#"{"error":"rate_limited"}"#.to_vec(),
        }),
        Ok(TransportResponse {
            status: 200,
            body: br#"{
                "model": "jev-1.14.0",
                "answers": {
                    "difficulty": {
                        "type": "choice",
                        "choice": "easy",
                        "confidence": 0.95,
                        "probabilities": {
                            "easy": 0.95,
                            "medium": 0.03,
                            "hard": 0.01,
                            "extreme": 0.01
                        }
                    }
                },
                "usage": {"input_tokens": 80, "output_tokens": 8}
            }"#
            .to_vec(),
        }),
    ]);
    let failing_client =
        DecisionClient::with_transport(failing_vault.clone(), transport.clone()).expect("client");

    for (scope, req) in [
        (&scope_openai_a, &req_openai_alt),
        (&scope_openai_a, &req_openai_pinned),
        (&scope_openai_a, &req_jev_1),
        (&scope_jev, &req_openai_1),
    ] {
        let err = classify_advice(scope, req, false, failing_vault.clone(), &failing_client)
            .await
            .expect_err("mismatch must fail before vault/network");
        assert_eq!(err.code(), "profile_advisor_mismatch");
    }
    assert!(failing_vault.recorded_reads().is_empty());
    assert!(transport.recorded_requests().is_empty());
    assert!(!fixture.cache_dir.exists());

    // 3. Missing OpenAI credential (even when Jev credential exists) and unavailable vault
    //    fail closed without falling back to Jev or invoking transport.
    let jev_only_vault = CliMockVault::with_secrets(&[("jev-api", jev_secret)]);
    let jev_only_client =
        DecisionClient::with_transport(jev_only_vault.clone(), transport.clone()).expect("client");
    let no_cred_err = classify_advice(
        &scope_openai_a,
        &req_openai_1,
        false,
        jev_only_vault.clone(),
        &jev_only_client,
    )
    .await
    .expect_err("missing openai credential must fail without using jev secret");
    assert_eq!(no_cred_err.code(), "not_configured");
    assert_eq!(
        jev_only_vault.recorded_reads(),
        vec!["openai-decisions".to_string()]
    );

    let unavail_err = classify_advice(
        &scope_openai_a,
        &req_openai_1,
        false,
        failing_vault.clone(),
        &failing_client,
    )
    .await
    .expect_err("unavailable vault must fail before transport");
    assert_eq!(unavail_err.code(), "vault_unavailable");
    assert!(transport.recorded_requests().is_empty());

    // 4. Positive OpenAI profile classify routes to OPENAI_CLASSIFY_URL without effort,
    //    keeps disagreeing baseline comparison side-effect-free to registry/routing,
    //    and resumes from cache with zero vault or network calls (library + CLI subprocess).
    let both_vault =
        CliMockVault::with_secrets(&[("openai-decisions", openai_secret), ("jev-api", jev_secret)]);
    let both_client =
        DecisionClient::with_transport(both_vault.clone(), transport.clone()).expect("client");

    let first_a = classify_advice(
        &scope_openai_a,
        &req_openai_1,
        false,
        both_vault.clone(),
        &both_client,
    )
    .await
    .expect("profile-openai classify should succeed");
    assert!(first_a.untrusted_data);
    assert!(first_a.advisory_only);
    assert_eq!(first_a.mode, "shadow");
    assert_eq!(first_a.source, AdviceSource::Provider);
    assert!(first_a.provider_call_performed);
    assert_eq!(first_a.profile_id.as_deref(), Some("profile-openai"));
    assert_eq!(first_a.cache_key, prev_openai_a.cache_key);
    assert_eq!(first_a.advice.provider, DecisionProvider::Openai);
    assert_eq!(first_a.advice.difficulty, Some(Difficulty::Extreme));
    assert_eq!(first_a.agrees_with_baseline, Some(false));
    assert_eq!(transport.recorded_requests().len(), 1);
    assert_eq!(
        transport.recorded_requests()[0].provider,
        DecisionProvider::Openai
    );
    assert_eq!(transport.recorded_requests()[0].url, OPENAI_CLASSIFY_URL);
    assert_eq!(
        transport.recorded_requests()[0].method,
        TransportMethod::Post
    );
    assert_eq!(transport.recorded_requests()[0].bearer_token, openai_secret);
    let sent_body: serde_json::Value = serde_json::from_str(
        transport.recorded_requests()[0]
            .json_body
            .as_deref()
            .expect("openai body"),
    )
    .expect("parse openai body");
    assert_eq!(sent_body["model"], "gpt-6-luna");
    assert!(sent_body.get("effort").is_none());
    assert!(sent_body.get("reasoning").is_none());

    // Even a 0.97 confident disagreeing comparison must never mutate registry or routing.
    assert_eq!(
        fs::read(&registry_file).expect("read registry bytes after"),
        reg_bytes_before
    );

    let record_a_path = fixture
        .cache_dir
        .join(format!("{}.json", first_a.cache_key));
    let record_a_text = fs::read_to_string(&record_a_path).expect("read profile-openai cache");
    assert!(!record_a_text.contains(raw_summary));
    assert!(!record_a_text.contains(openai_secret));
    let record_a_val: serde_json::Value =
        serde_json::from_str(&record_a_text).expect("parse profile-openai cache");
    assert_eq!(record_a_val["profile_id"], "profile-openai");
    assert_eq!(record_a_val["provider"], "openai");

    let failing_reads_before = failing_vault.recorded_reads().len();
    let hit_a = classify_advice(
        &scope_openai_a,
        &req_openai_1,
        false,
        failing_vault.clone(),
        &failing_client,
    )
    .await
    .expect("profile-openai cache resume must succeed without vault/network");
    assert_eq!(hit_a.source, AdviceSource::Cache);
    assert_eq!(hit_a.profile_id.as_deref(), Some("profile-openai"));
    assert!(!hit_a.provider_call_performed);
    assert_eq!(hit_a.advice, first_a.advice);
    assert_eq!(hit_a.agrees_with_baseline, Some(false));
    assert_eq!(failing_vault.recorded_reads().len(), failing_reads_before);
    assert_eq!(transport.recorded_requests().len(), 1);

    let cli_hit_a_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-openai")
        .arg("classify")
        .arg("--request")
        .arg(&req_openai_1)
        .output()
        .expect("run subprocess profile-openai cache resume");
    assert!(
        cli_hit_a_cmd.status.success(),
        "subprocess profile-openai cache resume failed: {}",
        String::from_utf8_lossy(&cli_hit_a_cmd.stderr)
    );
    let cli_hit_a_stdout = String::from_utf8_lossy(&cli_hit_a_cmd.stdout);
    assert!(!cli_hit_a_stdout.contains(raw_summary));
    assert!(!cli_hit_a_stdout.contains(openai_secret));
    let cli_hit_a: AdviceClassifyOutput =
        serde_json::from_slice(&cli_hit_a_cmd.stdout).expect("parse subprocess hit a");
    assert_eq!(cli_hit_a.source, AdviceSource::Cache);
    assert_eq!(cli_hit_a.profile_id.as_deref(), Some("profile-openai"));
    assert!(!cli_hit_a.provider_call_performed);
    assert_eq!(cli_hit_a.advice.provider, DecisionProvider::Openai);
    assert_eq!(cli_hit_a.advice.difficulty, Some(Difficulty::Extreme));
    assert_eq!(cli_hit_a.agrees_with_baseline, Some(false));

    // 5. Distinct OpenAI profile (`profile-openai-b`) and explicit unscoped OpenAI (`profile_id: None`)
    //    stay isolated from `profile-openai`'s cache entry and resume independently.
    let first_b = classify_advice(
        &scope_openai_b,
        &req_openai_1,
        false,
        both_vault.clone(),
        &both_client,
    )
    .await
    .expect("profile-openai-b refusal should succeed");
    assert_eq!(first_b.source, AdviceSource::Provider);
    assert_eq!(first_b.profile_id.as_deref(), Some("profile-openai-b"));
    assert_eq!(first_b.advice.reason_code, "refusal");
    assert!(first_b.advice.needs_review);
    assert_eq!(first_b.agrees_with_baseline, None);
    assert_eq!(transport.recorded_requests().len(), 2);

    let cli_hit_b_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-openai-b")
        .arg("classify")
        .arg("--request")
        .arg(&req_openai_1)
        .output()
        .expect("run subprocess profile-openai-b cached refusal");
    assert!(cli_hit_b_cmd.status.success());
    let cli_hit_b: AdviceClassifyOutput =
        serde_json::from_slice(&cli_hit_b_cmd.stdout).expect("parse subprocess hit b");
    assert_eq!(cli_hit_b.source, AdviceSource::Cache);
    assert_eq!(cli_hit_b.profile_id.as_deref(), Some("profile-openai-b"));
    assert_eq!(cli_hit_b.advice.reason_code, "refusal");
    assert_eq!(cli_hit_b.agrees_with_baseline, None);

    let first_unscoped = classify_advice(
        &scope_unscoped,
        &req_openai_1,
        false,
        both_vault.clone(),
        &both_client,
    )
    .await
    .expect("unscoped openai classify must stay isolated from profile-openai");
    assert_eq!(first_unscoped.source, AdviceSource::Provider);
    assert_eq!(first_unscoped.profile_id, None);
    assert_eq!(first_unscoped.advice.difficulty, Some(Difficulty::Medium));
    assert_eq!(transport.recorded_requests().len(), 3);

    let cli_hit_unscoped_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("classify")
        .arg("--request")
        .arg(&req_openai_1)
        .output()
        .expect("run subprocess unscoped openai cache resume");
    assert!(cli_hit_unscoped_cmd.status.success());
    let cli_hit_unscoped: AdviceClassifyOutput =
        serde_json::from_slice(&cli_hit_unscoped_cmd.stdout).expect("parse unscoped hit");
    assert_eq!(cli_hit_unscoped.source, AdviceSource::Cache);
    assert_eq!(cli_hit_unscoped.profile_id, None);
    assert_eq!(cli_hit_unscoped.advice.difficulty, Some(Difficulty::Medium));

    // 6. Rate-limit on `profile-openai` makes 1 attempt, seals Uncertain, blocks replay
    //    without `--refresh`, and does not block `profile-jev` on the same task.
    let rate_err = classify_advice(
        &scope_openai_a,
        &req_openai_2,
        false,
        both_vault.clone(),
        &both_client,
    )
    .await
    .expect_err("429 on profile-openai must fail without retry");
    assert_eq!(rate_err.code(), "rate_limited");
    assert_eq!(transport.recorded_requests().len(), 4);

    let cli_uncertain_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-openai")
        .arg("classify")
        .arg("--request")
        .arg(&req_openai_2)
        .output()
        .expect("run subprocess profile-openai uncertain refusal");
    assert!(!cli_uncertain_cmd.status.success());
    let cli_uncertain_err: AdviceError =
        serde_json::from_slice(&cli_uncertain_cmd.stderr).expect("parse uncertain err");
    assert_eq!(
        cli_uncertain_err.code(),
        "uncertain_attempt_requires_refresh"
    );

    let jev_out = classify_advice(
        &scope_jev,
        &req_jev_2,
        false,
        both_vault.clone(),
        &both_client,
    )
    .await
    .expect("profile-jev must remain isolated from profile-openai uncertain state");
    assert_eq!(jev_out.source, AdviceSource::Provider);
    assert_eq!(jev_out.profile_id.as_deref(), Some("profile-jev"));
    assert_eq!(jev_out.advice.provider, DecisionProvider::Jev);
    assert_eq!(transport.recorded_requests().len(), 5);
    assert_eq!(transport.recorded_requests()[4].url, JEV_CLASSIFY_URL);

    // 7. Cross-profile tampering on `profile-openai`'s cache file is refused with `corrupt_cache`.
    let mut tampered = record_a_val;
    tampered["profile_id"] = serde_json::Value::String("profile-openai-b".to_string());
    fs::write(
        &record_a_path,
        serde_json::to_vec_pretty(&tampered).expect("serialize tampered profile record"),
    )
    .expect("write tampered profile record");
    fs::set_permissions(&record_a_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 tampered profile record");

    let cli_tampered_cmd = Command::new(advice_bin())
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--cache-dir")
        .arg(&fixture.cache_dir)
        .arg("--project-root")
        .arg(&fixture.project_root)
        .arg("--context-id")
        .arg(context_id)
        .arg("--profile-id")
        .arg("profile-openai")
        .arg("classify")
        .arg("--request")
        .arg(&req_openai_1)
        .output()
        .expect("run subprocess tampered profile cache refusal");
    assert!(!cli_tampered_cmd.status.success());
    let cli_tampered_err: AdviceError =
        serde_json::from_slice(&cli_tampered_cmd.stderr).expect("parse tampered profile err");
    assert_eq!(cli_tampered_err.code(), "corrupt_cache");

    // Registry file remained completely untouched throughout all profile classifications.
    assert_eq!(
        fs::read(&registry_file).expect("read final registry bytes"),
        reg_bytes_before
    );
}
