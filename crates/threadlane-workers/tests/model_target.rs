use std::collections::BTreeSet;
use std::path::PathBuf;

use threadlane_workers::{
    resolve_model_target, ExecutionBudgets, WorkerError, WorkerFilePermissions, WorkerProvider,
    WorkerSpec,
};

fn spec(provider: WorkerProvider, model: &str, effort: Option<&str>) -> WorkerSpec {
    WorkerSpec {
        provider,
        executable: PathBuf::from("/usr/local/bin/worker"),
        model: model.to_string(),
        effort: effort.map(|value| value.to_string()),
        allowed_models: BTreeSet::new(),
        session_id: None,
        standalone: provider == WorkerProvider::OpenCodeV2,
        repository_path: PathBuf::from("/tmp/repository"),
        base_commit: "HEAD".to_string(),
        work_area_root: PathBuf::from("/tmp/work-area"),
        allowed_write_paths: BTreeSet::new(),
        prompt: "test prompt".to_string(),
        file_permissions: WorkerFilePermissions::default(),
        budgets: ExecutionBudgets::default(),
    }
}

#[test]
fn canonical_resolver_matches_worker_spec_and_rejects_missing_agy_effort() {
    let target = resolve_model_target(
        WorkerProvider::Antigravity,
        "gemini-4-argon-high",
        Some("high"),
    )
    .expect("valid antigravity target");
    assert_eq!(target.base_model, "gemini-4-argon-high");
    assert_eq!(target.effort.as_deref(), Some("high"));
    assert_eq!(target.cli_model_arg, "gemini-4-argon-high");
    assert_eq!(
        spec(
            WorkerProvider::Antigravity,
            "gemini-4-argon-high",
            Some("high")
        )
        .resolve_model_target()
        .expect("delegated antigravity target"),
        target
    );

    let missing_effort =
        resolve_model_target(WorkerProvider::Antigravity, "gemini-4-argon-high", None)
            .expect_err("antigravity without explicit effort must be rejected");
    assert!(matches!(missing_effort, WorkerError::InvalidSpec(_)));

    let invalid_effort = resolve_model_target(
        WorkerProvider::Antigravity,
        "gemini-4-argon-high",
        Some("-high"),
    )
    .expect_err("invalid antigravity effort format must be rejected");
    assert!(matches!(invalid_effort, WorkerError::InvalidSpec(_)));

    let invalid_model =
        resolve_model_target(WorkerProvider::Antigravity, "vendor/model", Some("high"))
            .expect_err("invalid antigravity model format must be rejected");
    assert!(matches!(invalid_model, WorkerError::InvalidSpec(_)));
}

#[test]
fn canonical_resolver_uses_opencode_inline_variant_as_effective_effort() {
    let target = resolve_model_target(
        WorkerProvider::OpenCodeV2,
        "opencode-go/deepseek-v4.1-flash#max",
        None,
    )
    .expect("valid inline OpenCode variant");
    assert_eq!(target.base_model, "opencode-go/deepseek-v4.1-flash");
    assert_eq!(target.effort.as_deref(), Some("max"));
    assert_eq!(target.cli_model_arg, "opencode-go/deepseek-v4.1-flash#max");
    assert_eq!(
        spec(
            WorkerProvider::OpenCodeV2,
            "opencode-go/deepseek-v4.1-flash#max",
            None
        )
        .resolve_model_target()
        .expect("delegated OpenCode target"),
        target
    );

    let conflict = resolve_model_target(
        WorkerProvider::OpenCodeV2,
        "opencode-go/deepseek-v4.1-flash#max",
        Some("high"),
    )
    .expect_err("conflicting inline variant and explicit effort must be rejected");
    assert!(matches!(conflict, WorkerError::InvalidSpec(_)));

    let invalid_model =
        resolve_model_target(WorkerProvider::OpenCodeV2, "plain-model", Some("max"))
            .expect_err("OpenCode model without provider/model format must be rejected");
    assert!(matches!(invalid_model, WorkerError::InvalidSpec(_)));
}
