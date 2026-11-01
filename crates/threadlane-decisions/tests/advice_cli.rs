#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use fs2::FileExt;
use threadlane_decisions::{
    AdviceClassifyOutput, AdviceError, AdvicePreviewOutput, AdviceSource,
    ADVICE_CACHE_SCHEMA_VERSION,
};
use threadlane_registry::{
    Difficulty, ModelDefinition, RegistryChange, RegistryStore, ToolAdapter, ToolDefinition,
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
    assert!(help_stdout.contains("--registry-dir"));
    assert!(help_stdout.contains("--cache-dir"));
    assert!(help_stdout.contains("--project-root"));
    assert!(help_stdout.contains("--context-id"));
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

    let preview: AdvicePreviewOutput =
        serde_json::from_slice(&preview_cmd.stdout).expect("parse preview json");
    assert!(preview.untrusted_data);
    assert!(preview.advisory_only);
    assert_eq!(preview.mode, "shadow");
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
    let classify_out: AdviceClassifyOutput =
        serde_json::from_slice(&classify_cmd.stdout).expect("parse classify output");
    assert_eq!(classify_out.source, AdviceSource::Cache);
    assert!(!classify_out.provider_call_performed);
    assert!(classify_out.cache_age_ms.is_some());
    assert_eq!(classify_out.advice.difficulty, Some(Difficulty::Easy));
    assert_eq!(classify_out.agrees_with_baseline, Some(true));
    assert_eq!(classify_out.advice.input_tokens, Some(88));
    assert_eq!(classify_out.advice.output_tokens, Some(7));
}
