use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;
use threadlane_native::{
    preflight, preflight_from_store, resolve_choice, Difficulty, ExecutionPolicy, NativeErrorCode,
    PolicyAllowRule, PreflightBlockReason, PreflightRequest, RegistryStore, RoleExecutionRoute,
    RolePreflightRequest, ToolAdapter, WorkflowProfile, WorkflowRole,
    EXECUTION_POLICY_SCHEMA_VERSION, MAX_POLICY_ALLOW_RULES, MAX_PREFLIGHT_ROLES,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, RegistryChange, RegistrySnapshot, RoleAssignment, ToolDefinition,
};

fn sample_tool(id: &str, adapter: ToolAdapter, enabled: bool) -> ToolDefinition {
    ToolDefinition {
        id: id.to_string(),
        label: format!("Tool {id}"),
        adapter,
        executable_path: if adapter.is_cli_adapter() {
            Some(PathBuf::from(format!("/opt/tools/{id}")))
        } else {
            None
        },
        enabled,
    }
}

fn sample_model(
    id: &str,
    tool_id: &str,
    model_arg: &str,
    efforts: &[&str],
    allow_no_effort: bool,
) -> ModelDefinition {
    ModelDefinition {
        id: id.to_string(),
        tool_id: tool_id.to_string(),
        label: format!("Model {id}"),
        model_arg: model_arg.to_string(),
        efforts: efforts.iter().map(|s| (*s).to_string()).collect(),
        allow_no_effort,
    }
}

fn base_snapshot() -> RegistrySnapshot {
    RegistrySnapshot {
        schema_version: 1,
        revision: 7,
        tools: vec![
            sample_tool("tool-codex", ToolAdapter::CodexCli, true),
            sample_tool("tool-agy", ToolAdapter::Antigravity, true),
            sample_tool("tool-oc", ToolAdapter::OpencodeV2, true),
            sample_tool("tool-jev", ToolAdapter::JevApi, true),
            sample_tool("tool-decisions", ToolAdapter::OpenaiDecisionsApi, true),
            sample_tool("tool-git", ToolAdapter::Git, true),
            sample_tool("tool-tmux", ToolAdapter::Tmux, true),
            sample_tool("tool-codegraph", ToolAdapter::Codegraph, true),
        ],
        models: vec![
            sample_model(
                "model-native-chat",
                "tool-codex",
                "vendor-native-conversational-v1",
                &["high", "max"],
                false,
            ),
            sample_model(
                "model-headless-alpha",
                "tool-agy",
                "vendor-a/headless-alpha-v1",
                &["medium", "high"],
                false,
            ),
            sample_model(
                "model-headless-beta",
                "tool-oc",
                "vendor-b/headless-beta-v2",
                &["max"],
                true,
            ),
            sample_model("model-jev", "tool-jev", "jev-shadow-v1", &["low"], true),
            sample_model(
                "model-decisions",
                "tool-decisions",
                "decisions-v1",
                &["high"],
                false,
            ),
            sample_model("model-git", "tool-git", "git-helper", &[], true),
            sample_model("model-tmux", "tool-tmux", "tmux-helper", &[], true),
            sample_model("model-cg", "tool-codegraph", "cg-helper", &[], true),
        ],
        profiles: vec![
            WorkflowProfile {
                id: "default".to_string(),
                name: "default".to_string(),
                assignments: vec![RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "model-headless-alpha".to_string(),
                        effort: Some("high".to_string()),
                    },
                }],
            },
            WorkflowProfile {
                id: "profile-active".to_string(),
                name: "Active Team Profile".to_string(),
                assignments: vec![
                    RoleAssignment {
                        role: WorkflowRole::Follower,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "model-native-chat".to_string(),
                            effort: Some("max".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "model-headless-beta".to_string(),
                            effort: Some("max".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "model-headless-alpha".to_string(),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Reviewer,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "model-headless-alpha".to_string(),
                            effort: Some("high".to_string()),
                        },
                    },
                ],
            },
        ],
        default_profile_id: None,
    }
}

fn allow_all_test_models_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-test-allow".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "model-native-chat".to_string(),
                efforts: vec!["high".to_string(), "max".to_string()],
                allow_no_effort: false,
                roles: vec![],
            },
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-headless-alpha".to_string(),
                efforts: vec!["medium".to_string(), "high".to_string()],
                allow_no_effort: false,
                roles: vec![],
            },
            PolicyAllowRule {
                tool_id: "tool-oc".to_string(),
                model_id: "model-headless-beta".to_string(),
                efforts: vec!["max".to_string()],
                allow_no_effort: true,
                roles: vec![],
            },
        ],
    }
}

#[test]
fn missing_real_default_despite_profile_named_default_and_explicit_default_selection() {
    let mut snap = base_snapshot();
    let policy = allow_all_test_models_policy();

    // 1. Even though a profile with id="default" and name="default" exists, default_profile_id is None.
    // Requesting profile_id: None must report MissingDefaultProfile and never select "default".
    let req_default = PreflightRequest {
        profile_id: None,
        roles: vec![RolePreflightRequest::new(
            WorkflowRole::Coder,
            Some(Difficulty::Hard),
        )],
    };
    let report_missing_def = preflight(&snap, &req_default, &policy).expect("preflight");
    assert!(!report_missing_def.ready);
    assert!(report_missing_def.used_default_profile);
    assert_eq!(report_missing_def.profile_id, None);
    assert_eq!(report_missing_def.roles.len(), 1);
    assert!(!report_missing_def.roles[0].ready);
    assert_eq!(
        report_missing_def.roles[0].block_reason,
        Some(PreflightBlockReason::MissingDefaultProfile)
    );
    assert_eq!(report_missing_def.roles[0].selected, None);

    // 2. Explicitly requesting profile_id: Some("default") selects that profile by explicit ID.
    let req_explicit_named = PreflightRequest {
        profile_id: Some("default".to_string()),
        roles: vec![RolePreflightRequest::new(
            WorkflowRole::Coder,
            Some(Difficulty::Hard),
        )],
    };
    let report_explicit = preflight(&snap, &req_explicit_named, &policy).expect("preflight");
    assert!(report_explicit.ready);
    assert!(!report_explicit.used_default_profile);
    assert_eq!(report_explicit.profile_id.as_deref(), Some("default"));
    assert!(report_explicit.roles[0].ready);

    // 3. Setting default_profile_id to "profile-active" and requesting profile_id: None selects "profile-active".
    snap.default_profile_id = Some("profile-active".to_string());
    let report_real_def = preflight(&snap, &req_default, &policy).expect("preflight");
    assert!(report_real_def.ready);
    assert!(report_real_def.used_default_profile);
    assert_eq!(
        report_real_def.profile_id.as_deref(),
        Some("profile-active")
    );

    // 4. Requesting a nonexistent explicit profile reports MissingProfile.
    let req_missing_prof = PreflightRequest {
        profile_id: Some("does-not-exist".to_string()),
        roles: vec![RolePreflightRequest::new(
            WorkflowRole::Coder,
            Some(Difficulty::Hard),
        )],
    };
    let report_missing_prof = preflight(&snap, &req_missing_prof, &policy).expect("preflight");
    assert!(!report_missing_prof.ready);
    assert!(!report_missing_prof.used_default_profile);
    assert_eq!(
        report_missing_prof.profile_id.as_deref(),
        Some("does-not-exist")
    );
    assert_eq!(
        report_missing_prof.roles[0].block_reason,
        Some(PreflightBlockReason::MissingProfile)
    );
    assert_eq!(report_missing_prof.roles[0].selected, None);
}

#[test]
fn unrelated_unavailable_roles_do_not_block_requested_coder() {
    let mut snap = base_snapshot();
    // Disable tool-codex (used by Follower) and add broken Architect / Closure assignments.
    snap.tools
        .iter_mut()
        .find(|t| t.id == "tool-codex")
        .expect("tool-codex")
        .enabled = false;

    let active = snap
        .profiles
        .iter_mut()
        .find(|p| p.id == "profile-active")
        .expect("profile-active");
    active.assignments.push(RoleAssignment {
        role: WorkflowRole::Architect,
        difficulty: None,
        choice: ModelChoice {
            model_id: "missing-architect-model".to_string(),
            effort: Some("high".to_string()),
        },
    });
    active.assignments.push(RoleAssignment {
        role: WorkflowRole::Closure,
        difficulty: None,
        choice: ModelChoice {
            model_id: "model-git".to_string(),
            effort: None,
        },
    });

    // Policy only allows Coder on tool-agy / model-headless-alpha / high.
    let coder_only_policy = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-coder-only".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-headless-alpha".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Coder],
        }],
    };

    let req = PreflightRequest {
        profile_id: Some("profile-active".to_string()),
        roles: vec![RolePreflightRequest::new(
            WorkflowRole::Coder,
            Some(Difficulty::Hard),
        )],
    };
    let report = preflight(&snap, &req, &coder_only_policy).expect("preflight coder only");
    assert!(report.ready);
    assert_eq!(report.roles.len(), 1);
    let sel = report.roles[0].selected.as_ref().expect("selected coder");
    assert_eq!(sel.tool_id, "tool-agy");
    assert_eq!(sel.model_id, "model-headless-alpha");
    assert_eq!(sel.model_arg, "vendor-a/headless-alpha-v1");
    assert_eq!(sel.effort.as_deref(), Some("high"));
    assert_eq!(sel.route, RoleExecutionRoute::HeadlessWorker);
    assert_eq!(sel.matched_difficulty, Some(Difficulty::Hard));
}

#[test]
fn difficulty_overrides_and_explicit_role_defaults_without_fallback() {
    let snap = base_snapshot();
    let policy = allow_all_test_models_policy();

    // Coder has Hard override (model-headless-alpha) and None default (model-headless-beta).
    // Reviewer has ONLY Hard override and NO role default.
    let req = PreflightRequest {
        profile_id: Some("profile-active".to_string()),
        roles: vec![
            RolePreflightRequest::new(WorkflowRole::Coder, Some(Difficulty::Hard)),
            RolePreflightRequest::new(WorkflowRole::Coder, Some(Difficulty::Easy)),
            RolePreflightRequest::new(WorkflowRole::Coder, None),
            RolePreflightRequest::new(WorkflowRole::Reviewer, Some(Difficulty::Easy)),
            RolePreflightRequest::new(WorkflowRole::Architect, Some(Difficulty::Hard)),
        ],
    };
    let report = preflight(&snap, &req, &policy).expect("preflight");
    assert!(!report.ready);
    assert_eq!(report.roles.len(), 5);

    // 1. Coder (Hard) -> exact difficulty match.
    let coder_hard = report.roles[0].selected.as_ref().expect("coder hard");
    assert_eq!(coder_hard.model_id, "model-headless-alpha");
    assert_eq!(coder_hard.matched_difficulty, Some(Difficulty::Hard));

    // 2. Coder (Easy) -> falls back to explicit role default (difficulty: None).
    let coder_easy = report.roles[1].selected.as_ref().expect("coder easy");
    assert_eq!(coder_easy.model_id, "model-headless-beta");
    assert_eq!(coder_easy.matched_difficulty, None);

    // 3. Coder (None) -> explicit role default.
    let coder_def = report.roles[2].selected.as_ref().expect("coder default");
    assert_eq!(coder_def, coder_easy);

    // 4. Reviewer (Easy) -> no Easy override and no role default; must not substitute Hard.
    assert!(!report.roles[3].ready);
    assert_eq!(
        report.roles[3].block_reason,
        Some(PreflightBlockReason::MissingAssignment)
    );
    assert_eq!(report.roles[3].selected, None);

    // 5. Architect (Hard) -> unassigned role; must report MissingAssignment.
    assert!(!report.roles[4].ready);
    assert_eq!(
        report.roles[4].block_reason,
        Some(PreflightBlockReason::MissingAssignment)
    );

    // Direct resolve_choice also matches.
    assert_eq!(
        resolve_choice(
            &snap,
            "profile-active",
            WorkflowRole::Coder,
            Some(Difficulty::Medium)
        )
        .expect("resolve_choice coder medium")
        .model_id,
        "model-headless-beta"
    );
}

#[test]
fn missing_and_disabled_tool_model_and_unsupported_effort() {
    let mut snap = base_snapshot();
    let policy = allow_all_test_models_policy();

    // Add a model pointing to a missing tool.
    snap.models.push(sample_model(
        "model-orphan-tool",
        "tool-does-not-exist",
        "orphan-arg",
        &["high"],
        false,
    ));

    // Disable tool-oc.
    snap.tools
        .iter_mut()
        .find(|t| t.id == "tool-oc")
        .expect("tool-oc")
        .enabled = false;

    snap.profiles.push(WorkflowProfile {
        id: "profile-broken".to_string(),
        name: "Broken Assignments Profile".to_string(),
        assignments: vec![
            RoleAssignment {
                role: WorkflowRole::Follower,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-does-not-exist".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Orchestrator,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-orphan-tool".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-headless-beta".to_string(),
                    effort: Some("max".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Reviewer,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-headless-alpha".to_string(),
                    effort: Some("unsupported-effort-tier".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Closure,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-headless-alpha".to_string(),
                    effort: None,
                },
            },
        ],
    });

    let req = PreflightRequest {
        profile_id: Some("profile-broken".to_string()),
        roles: vec![
            RolePreflightRequest::new(WorkflowRole::Follower, None),
            RolePreflightRequest::new(WorkflowRole::Orchestrator, None),
            RolePreflightRequest::new(WorkflowRole::Coder, None),
            RolePreflightRequest::new(WorkflowRole::Reviewer, None),
            RolePreflightRequest::new(WorkflowRole::Closure, None),
        ],
    };

    let report = preflight(&snap, &req, &policy).expect("preflight broken profile");
    assert!(!report.ready);
    assert_eq!(
        report.roles[0].block_reason,
        Some(PreflightBlockReason::MissingModel)
    );
    assert_eq!(
        report.roles[1].block_reason,
        Some(PreflightBlockReason::MissingTool)
    );
    assert_eq!(
        report.roles[2].block_reason,
        Some(PreflightBlockReason::ToolDisabled)
    );
    assert_eq!(
        report.roles[3].block_reason,
        Some(PreflightBlockReason::UnsupportedEffort)
    );
    assert_eq!(
        report.roles[4].block_reason,
        Some(PreflightBlockReason::UnsupportedEffort)
    );
    assert!(report.roles.iter().all(|r| r.selected.is_none()));
}

#[test]
fn native_handoff_headless_worker_and_unsupported_routes() {
    let mut snap = base_snapshot();
    snap.profiles.push(WorkflowProfile {
        id: "profile-routes".to_string(),
        name: "Routes Profile".to_string(),
        assignments: vec![
            RoleAssignment {
                role: WorkflowRole::Follower,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-native-chat".to_string(),
                    effort: Some("max".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "model-headless-alpha".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Easy),
                choice: ModelChoice {
                    model_id: "model-headless-beta".to_string(),
                    effort: None,
                },
            },
            RoleAssignment {
                role: WorkflowRole::Architect,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-jev".to_string(),
                    effort: Some("low".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Orchestrator,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-decisions".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Reviewer,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "model-git".to_string(),
                    effort: None,
                },
            },
            RoleAssignment {
                role: WorkflowRole::Closure,
                difficulty: Some(Difficulty::Easy),
                choice: ModelChoice {
                    model_id: "model-tmux".to_string(),
                    effort: None,
                },
            },
            RoleAssignment {
                role: WorkflowRole::Closure,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "model-cg".to_string(),
                    effort: None,
                },
            },
        ],
    });

    let mut policy = allow_all_test_models_policy();
    // Even if an operator policy lists API/auxiliary tools, unsupported routes remain blocked.
    policy.allow_rules.extend([
        PolicyAllowRule {
            tool_id: "tool-jev".to_string(),
            model_id: "model-jev".to_string(),
            efforts: vec!["low".to_string()],
            allow_no_effort: true,
            roles: vec![],
        },
        PolicyAllowRule {
            tool_id: "tool-decisions".to_string(),
            model_id: "model-decisions".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![],
        },
        PolicyAllowRule {
            tool_id: "tool-git".to_string(),
            model_id: "model-git".to_string(),
            efforts: vec![],
            allow_no_effort: true,
            roles: vec![],
        },
        PolicyAllowRule {
            tool_id: "tool-tmux".to_string(),
            model_id: "model-tmux".to_string(),
            efforts: vec![],
            allow_no_effort: true,
            roles: vec![],
        },
        PolicyAllowRule {
            tool_id: "tool-codegraph".to_string(),
            model_id: "model-cg".to_string(),
            efforts: vec![],
            allow_no_effort: true,
            roles: vec![],
        },
    ]);

    let req = PreflightRequest {
        profile_id: Some("profile-routes".to_string()),
        roles: vec![
            RolePreflightRequest::new(WorkflowRole::Follower, None),
            RolePreflightRequest::new(WorkflowRole::Coder, Some(Difficulty::Hard)),
            RolePreflightRequest::new(WorkflowRole::Coder, Some(Difficulty::Easy)),
            RolePreflightRequest::new(WorkflowRole::Architect, None),
            RolePreflightRequest::new(WorkflowRole::Orchestrator, None),
            RolePreflightRequest::new(WorkflowRole::Reviewer, None),
            RolePreflightRequest::new(WorkflowRole::Closure, Some(Difficulty::Easy)),
            RolePreflightRequest::new(WorkflowRole::Closure, Some(Difficulty::Hard)),
        ],
    };

    let report = preflight(&snap, &req, &policy).expect("preflight routes");
    assert!(!report.ready);

    // CodexCli -> NativeHandoff (never HeadlessWorker).
    let follower = report.roles[0].selected.as_ref().expect("follower ready");
    assert_eq!(follower.adapter, ToolAdapter::CodexCli);
    assert_eq!(follower.route, RoleExecutionRoute::NativeHandoff);
    assert_ne!(follower.route, RoleExecutionRoute::HeadlessWorker);

    // Antigravity & OpencodeV2 -> HeadlessWorker.
    let coder_agy = report.roles[1].selected.as_ref().expect("coder agy ready");
    assert_eq!(coder_agy.adapter, ToolAdapter::Antigravity);
    assert_eq!(coder_agy.route, RoleExecutionRoute::HeadlessWorker);

    let coder_oc = report.roles[2].selected.as_ref().expect("coder oc ready");
    assert_eq!(coder_oc.adapter, ToolAdapter::OpencodeV2);
    assert_eq!(coder_oc.route, RoleExecutionRoute::HeadlessWorker);
    assert_eq!(coder_oc.effort, None);

    // JevApi, OpenaiDecisionsApi, Git, Tmux, Codegraph -> UnsupportedRoute.
    for blocked in &report.roles[3..] {
        assert!(!blocked.ready);
        assert_eq!(
            blocked.block_reason,
            Some(PreflightBlockReason::UnsupportedRoute)
        );
        assert_eq!(blocked.selected, None);
    }
}

#[test]
fn deny_by_default_and_exact_allowlist_policy_enforcement() {
    let snap = base_snapshot();

    // 1. Empty allow_rules is deny-by-default and blocks all requested roles with PolicyBlocked.
    let deny_all = ExecutionPolicy::deny_all("policy-empty").expect("valid deny-all policy");
    assert!(deny_all.allow_rules.is_empty());
    assert_eq!(deny_all.digest().expect("digest").len(), 64);

    let req = PreflightRequest {
        profile_id: Some("profile-active".to_string()),
        roles: vec![
            RolePreflightRequest::new(WorkflowRole::Follower, None),
            RolePreflightRequest::new(WorkflowRole::Coder, Some(Difficulty::Hard)),
            RolePreflightRequest::new(WorkflowRole::Reviewer, Some(Difficulty::Hard)),
        ],
    };
    let denied_report = preflight(&snap, &req, &deny_all).expect("preflight deny-all");
    assert!(!denied_report.ready);
    for r in &denied_report.roles {
        assert!(!r.ready);
        assert_eq!(r.block_reason, Some(PreflightBlockReason::PolicyBlocked));
        assert_eq!(r.selected, None);
    }

    // 2. Role-restricted and effort-restricted policy:
    // Allow model-headless-alpha ONLY for Coder with effort "high", and for Reviewer ONLY with effort "medium".
    // In profile-active, Reviewer (Hard) uses effort "high", so Coder (Hard) is allowed while Reviewer (Hard) is blocked!
    let scoped_policy = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-scoped".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-headless-alpha".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Coder],
            },
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-headless-alpha".to_string(),
                efforts: vec!["medium".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Reviewer],
            },
        ],
    };

    let scoped_report = preflight(&snap, &req, &scoped_policy).expect("preflight scoped");
    assert!(!scoped_report.ready);
    // Follower -> not in allow_rules -> PolicyBlocked.
    assert_eq!(
        scoped_report.roles[0].block_reason,
        Some(PreflightBlockReason::PolicyBlocked)
    );
    // Coder (Hard) -> tool-agy / model-headless-alpha / high / Coder -> Ready!
    assert!(scoped_report.roles[1].ready);
    assert_eq!(
        scoped_report.roles[1]
            .selected
            .as_ref()
            .expect("coder selected")
            .model_id,
        "model-headless-alpha"
    );
    // Reviewer (Hard) -> configured effort is "high", but rule for Reviewer only allows "medium" -> PolicyBlocked!
    assert!(!scoped_report.roles[2].ready);
    assert_eq!(
        scoped_report.roles[2].block_reason,
        Some(PreflightBlockReason::PolicyBlocked)
    );
    assert_eq!(scoped_report.roles[2].selected, None);
}

#[test]
fn deterministic_policy_digest_and_malformed_or_duplicate_requests_rejected() {
    let snap = base_snapshot();

    // 1. Reordering rules, roles, or efforts yields the exact same canonical policy digest.
    let policy_a = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-canon".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-oc".to_string(),
                model_id: "model-headless-beta".to_string(),
                efforts: vec!["max".to_string(), "high".to_string()],
                allow_no_effort: true,
                roles: vec![WorkflowRole::Reviewer, WorkflowRole::Coder],
            },
            PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "model-native-chat".to_string(),
                efforts: vec!["max".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Follower],
            },
        ],
    };
    let policy_b = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "policy-canon".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "model-native-chat".to_string(),
                efforts: vec!["max".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Follower],
            },
            PolicyAllowRule {
                tool_id: "tool-oc".to_string(),
                model_id: "model-headless-beta".to_string(),
                efforts: vec!["high".to_string(), "max".to_string()],
                allow_no_effort: true,
                roles: vec![WorkflowRole::Coder, WorkflowRole::Reviewer],
            },
        ],
    };
    let digest_a = policy_a.digest().expect("digest_a");
    let digest_b = policy_b.digest().expect("digest_b");
    assert_eq!(digest_a, digest_b);

    let mut policy_changed = policy_a.clone();
    policy_changed.allow_rules[0].allow_no_effort = false;
    assert_ne!(
        digest_a,
        policy_changed.digest().expect("digest_changed")
    );

    // 2. Malformed policies are rejected.
    let bad_schema = ExecutionPolicy {
        schema_version: 99,
        ..policy_a.clone()
    };
    assert_eq!(
        bad_schema.validate().unwrap_err().error_code(),
        NativeErrorCode::ValidationFailed
    );

    let vacuous_rule = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "pol-vacuous".to_string(),
        allow_rules: vec![PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-headless-alpha".to_string(),
            efforts: vec![],
            allow_no_effort: false,
            roles: vec![],
        }],
    };
    assert_eq!(
        vacuous_rule.validate().unwrap_err().error_code(),
        NativeErrorCode::ValidationFailed
    );

    let overlapping_rules = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "pol-overlap".to_string(),
        allow_rules: vec![
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-headless-alpha".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![],
            },
            PolicyAllowRule {
                tool_id: "tool-agy".to_string(),
                model_id: "model-headless-alpha".to_string(),
                efforts: vec!["medium".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Coder],
            },
        ],
    };
    assert_eq!(
        overlapping_rules.validate().unwrap_err().error_code(),
        NativeErrorCode::ValidationFailed
    );

    let too_many_rules = ExecutionPolicy {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        policy_id: "pol-many".to_string(),
        allow_rules: (0..=MAX_POLICY_ALLOW_RULES)
            .map(|idx| PolicyAllowRule {
                tool_id: format!("tool-{idx}"),
                model_id: format!("model-{idx}"),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![],
            })
            .collect(),
    };
    assert_eq!(
        too_many_rules.validate().unwrap_err().error_code(),
        NativeErrorCode::ValidationFailed
    );

    // 3. Malformed / duplicate / overlong preflight requests are rejected.
    let empty_roles = PreflightRequest {
        profile_id: Some("profile-active".to_string()),
        roles: vec![],
    };
    assert_eq!(
        preflight(&snap, &empty_roles, &policy_a)
            .unwrap_err()
            .error_code(),
        NativeErrorCode::ValidationFailed
    );

    let dup_roles = PreflightRequest {
        profile_id: Some("profile-active".to_string()),
        roles: vec![
            RolePreflightRequest::new(WorkflowRole::Coder, Some(Difficulty::Hard)),
            RolePreflightRequest::new(WorkflowRole::Coder, Some(Difficulty::Hard)),
        ],
    };
    assert_eq!(
        preflight(&snap, &dup_roles, &policy_a)
            .unwrap_err()
            .error_code(),
        NativeErrorCode::ValidationFailed
    );

    let overlong_roles = PreflightRequest {
        profile_id: Some("profile-active".to_string()),
        roles: (0..=MAX_PREFLIGHT_ROLES)
            .map(|_| RolePreflightRequest::new(WorkflowRole::Coder, None))
            .collect(),
    };
    assert_eq!(
        preflight(&snap, &overlong_roles, &policy_a)
            .unwrap_err()
            .error_code(),
        NativeErrorCode::ValidationFailed
    );

    let overlong_profile_id = PreflightRequest {
        profile_id: Some("p".repeat(129)),
        roles: vec![RolePreflightRequest::new(WorkflowRole::Coder, None)],
    };
    assert_eq!(
        preflight(&snap, &overlong_profile_id, &policy_a)
            .unwrap_err()
            .error_code(),
        NativeErrorCode::ValidationFailed
    );

    // Caller override fields in JSON must be rejected by #[serde(deny_unknown_fields)].
    let override_attempt =
        r#"{"role":"coder","difficulty":"hard","model_arg":"unapproved-override"}"#;
    assert!(serde_json::from_str::<RolePreflightRequest>(override_attempt).is_err());
}

#[test]
fn preflight_and_preflight_from_store_are_pure_and_perform_no_file_writes() {
    let tmp = TempDir::new().expect("tempdir");
    let reg_dir = tmp.path().join("registry");
    let store = RegistryStore::open(&reg_dir).expect("open registry store");

    let s1 = store
        .apply(
            0,
            RegistryChange::UpsertTool {
                tool: sample_tool("tool-agy", ToolAdapter::Antigravity, true),
            },
        )
        .expect("upsert tool");
    let s2 = store
        .apply(
            s1.revision,
            RegistryChange::UpsertModel {
                model: sample_model(
                    "model-headless-alpha",
                    "tool-agy",
                    "vendor-a/headless-alpha-v1",
                    &["high"],
                    false,
                ),
            },
        )
        .expect("upsert model");
    let s3 = store
        .apply(
            s2.revision,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    id: "profile-store".to_string(),
                    name: "Store Profile".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "model-headless-alpha".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("upsert profile");

    let before_bytes = fs::read(store.registry_path()).expect("read registry.json before");
    let before_entries: Vec<_> = fs::read_dir(tmp.path())
        .expect("read tmp dir")
        .map(|e| e.expect("entry").file_name())
        .collect();

    let policy = allow_all_test_models_policy();
    let req = PreflightRequest {
        profile_id: Some("profile-store".to_string()),
        roles: vec![RolePreflightRequest::new(
            WorkflowRole::Coder,
            Some(Difficulty::Hard),
        )],
    };
    let report = preflight_from_store(&store, &req, &policy).expect("preflight_from_store");
    assert!(report.ready);
    assert_eq!(report.registry_revision, s3.revision);

    let after_bytes = fs::read(store.registry_path()).expect("read registry.json after");
    let after_entries: Vec<_> = fs::read_dir(tmp.path())
        .expect("read tmp dir after")
        .map(|e| e.expect("entry").file_name())
        .collect();
    assert_eq!(before_bytes, after_bytes);
    assert_eq!(before_entries, after_entries);
}
