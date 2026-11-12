#[cfg(unix)]
mod unix_workspace_tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::env;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::{symlink, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use threadlane_authority::{
        compute_record_subject_digest, derive_approval_id, verify_approval_record_integrity,
        ApprovalRecord, ApprovedTaskRecord, APPROVALS_FILE_NAME, AUTHORITY_POLICY_VERSION,
        AUTHORITY_SCHEMA_VERSION,
    };
    use threadlane_core::observation::{
        current_observation_time_ms, serialize_workspace_native_sample_sidecar, ObservationJob,
        ObservationJobState, ObservationSnapshotRequest, ObservationSourceAvailability,
        ObservationTask, WorkspaceAcceptanceStatus, WorkspaceActivityState, WorkspaceAttentionKind,
        WorkspaceAvailabilityState, WorkspaceCapabilityState, WorkspaceChatIdentity,
        WorkspaceConfiguredSelection, WorkspaceConnectionState,
        WorkspaceExternalResolutionApplicability, WorkspaceExternalResolutionScope,
        WorkspaceFreshnessState, WorkspaceMessageSourceKind, WorkspaceNativeCapabilities,
        WorkspaceNativeSample, WorkspaceSampleConnection, WorkspaceSelectionEvidence,
    };
    use threadlane_core::Core;
    use threadlane_native::coworker_observation::{
        read_scoped_workspace_job_histories, WorkspaceHistoryDiagnosticKind, WorkspaceHistoryStores,
    };
    use threadlane_native::coworker_workflow::{
        sha256_hex, AcceptedPredecessorReceiptRef, CandidateCheckEvidence, CanonicalProjectBaseRef,
        CombinedCandidateIdentity, CombinedCandidateStatus, CoworkerBindingLedger,
        CoworkerJobPhase, CoworkerJobRecord, CoworkerTaskRecord, CoworkerTaskStage,
        CoworkerWorkflowStore, FrozenApprovalLinkage, PlanningAdviceGrant, PlanningCallBudget,
        PlanningCallReservation, PlanningGrantStatus, PlanningGrantUsage,
        PlanningReadOnlyCapabilities, PlanningReservationStatus, PlanningResultRef,
        ReviewVerdictOutcome, StructuredReviewVerdict, TaskCandidateRef, WorkBrief, WorkBriefKind,
        WorkflowHoldReason, WorkflowHoldRecord, COWORKER_WORKFLOW_SCHEMA_VERSION,
    };
    use threadlane_native::external_resolution::{
        ExternalResolutionConfirmation, ExternalResolutionEvidenceRef, ExternalResolutionScope,
        ExternalResolutionService, ExternalResolutionTarget, RecordExternalResolutionRequest,
    };
    use threadlane_native::native_chat_observation::{
        compute_observation_source_id, write_workspace_native_sample_sidecar,
    };
    use threadlane_native::profile_flow::{
        ExecutionPolicy, PolicyAllowRule, RoleExecutionRoute, SelectedRoleChoice,
        EXECUTION_POLICY_SCHEMA_VERSION,
    };
    use threadlane_plans::{
        compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanRevision,
        PlanStore, PlanTask, RegistrySnapshot, RoleAssignment, ToolAdapter, ToolDefinition,
        WorkflowProfile, WorkflowRole,
    };

    use crate::observation::{ObservationService, ObservationSourceConfig};

    static WORKSPACE_TEST_SEQ: AtomicU64 = AtomicU64::new(1);

    struct TestTree {
        root: PathBuf,
    }

    impl TestTree {
        fn new(label: &str) -> Self {
            let base = fs::canonicalize(env::temp_dir()).unwrap_or_else(|_| env::temp_dir());
            loop {
                let seq = WORKSPACE_TEST_SEQ.fetch_add(1, Ordering::Relaxed);
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                let root = base.join(format!(
                    "threadlane-workspace-obs-{label}-{}-{seq}-{nanos}",
                    std::process::id()
                ));
                match fs::create_dir(&root) {
                    Ok(()) => {
                        set_mode(&root, 0o700);
                        return Self { root };
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) => panic!("create test root {}: {err}", root.display()),
                }
            }
        }

        fn path(&self) -> &Path {
            &self.root
        }

        fn join(&self, relative: &str) -> PathBuf {
            self.root.join(relative)
        }

        fn dir(&self, relative: &str) -> PathBuf {
            let mut cur = self.root.clone();
            for part in relative.split('/') {
                cur.push(part);
                match fs::create_dir(&cur) {
                    Ok(()) => set_mode(&cur, 0o700),
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                        set_mode(&cur, 0o700)
                    }
                    Err(err) => panic!("create dir {}: {err}", cur.display()),
                }
            }
            cur
        }

        fn write_file(&self, relative: &str, bytes: &[u8], mode: u32) -> PathBuf {
            let path = self.join(relative);
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(mode)
                .open(&path)
                .unwrap_or_else(|err| panic!("open {}: {err}", path.display()));
            file.write_all(bytes)
                .unwrap_or_else(|err| panic!("write {}: {err}", path.display()));
            file.sync_all()
                .unwrap_or_else(|err| panic!("sync {}: {err}", path.display()));
            drop(file);
            set_mode(&path, mode);
            path
        }

        fn write_json(&self, relative: &str, value: &serde_json::Value) -> PathBuf {
            let bytes = serde_json::to_vec_pretty(value).expect("serialize JSON");
            self.write_file(relative, &bytes, 0o600)
        }
    }

    impl Drop for TestTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn set_mode(path: &Path, mode: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .unwrap_or_else(|err| panic!("chmod {mode:04o} {}: {err}", path.display()));
    }

    fn path_text(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    struct TestStores {
        state: PathBuf,
        registry: PathBuf,
        plans: PathBuf,
        authority: PathBuf,
        sessions: PathBuf,
    }

    impl TestStores {
        fn new(tree: &TestTree, prefix: &str) -> Self {
            Self {
                state: tree.dir(&format!("{prefix}/state")),
                registry: tree.dir(&format!("{prefix}/registry")),
                plans: tree.dir(&format!("{prefix}/plans")),
                authority: tree.dir(&format!("{prefix}/authority")),
                sessions: tree.dir(&format!("{prefix}/sessions")),
            }
        }

        fn canonical_source_id(&self) -> String {
            compute_observation_source_id(&self.plans, &self.authority, &self.sessions)
                .expect("compute canonical source_id")
        }

        fn config(&self, app_data_dir: &Path) -> ObservationSourceConfig {
            ObservationSourceConfig {
                runtime_state_dir: Some(self.state.clone()),
                registry_dir: self.registry.clone(),
                plans_dir: self.plans.clone(),
                authority_dir: self.authority.clone(),
                sessions_dir: self.sessions.clone(),
                app_data_dir: app_data_dir.to_path_buf(),
            }
        }

        fn context_json(
            &self,
            project_root: &Path,
            binding_id: &str,
            thread_uuid: &str,
        ) -> serde_json::Value {
            serde_json::json!({
                "schema_version": 1,
                "project_root": path_text(project_root),
                "binding_id": binding_id,
                "thread_uuid": thread_uuid,
                "route": "trusted_local_operator",
                "native_per_call_identity_verified": false,
                "stores": {
                    "state_dir": path_text(&self.state),
                    "registry_dir": path_text(&self.registry),
                    "plans_dir": path_text(&self.plans),
                    "authority_dir": path_text(&self.authority),
                    "sessions_dir": path_text(&self.sessions),
                }
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct EntryFingerprint {
        is_dir: bool,
        mode: u32,
        ino: u64,
        len: u64,
        mtime_sec: i64,
        mtime_nsec: i64,
        bytes: Vec<u8>,
    }

    fn snapshot_tree_fingerprints(root: &Path) -> BTreeMap<PathBuf, EntryFingerprint> {
        fn walk(dir: &Path, out: &mut BTreeMap<PathBuf, EntryFingerprint>) {
            for entry in fs::read_dir(dir).expect("read_dir") {
                let entry = entry.expect("dir entry");
                let path = entry.path();
                let meta = fs::symlink_metadata(&path).expect("symlink_metadata");
                let is_dir = meta.is_dir() && !meta.file_type().is_symlink();
                let bytes = if meta.is_file() && !meta.file_type().is_symlink() {
                    fs::read(&path).expect("read file bytes")
                } else {
                    Vec::new()
                };
                out.insert(
                    path.clone(),
                    EntryFingerprint {
                        is_dir,
                        mode: meta.permissions().mode() & 0o777,
                        ino: meta.ino(),
                        len: meta.len(),
                        mtime_sec: meta.mtime(),
                        mtime_nsec: meta.mtime_nsec(),
                        bytes,
                    },
                );
                if is_dir {
                    walk(&path, out);
                }
            }
        }

        let mut out = BTreeMap::new();
        walk(root, &mut out);
        out
    }

    #[derive(Clone, Copy)]
    struct SampleTiming {
        sequence: u64,
        observed_at_ms: i64,
        last_success_at_ms: i64,
    }

    fn make_native_sample(
        identity: WorkspaceChatIdentity,
        producer_epoch: &str,
        timing: SampleTiming,
        availability: WorkspaceAvailabilityState,
        activity: WorkspaceActivityState,
        selection: WorkspaceConfiguredSelection,
        diagnostic: Option<&str>,
    ) -> WorkspaceNativeSample {
        WorkspaceNativeSample {
            chat_identity: identity,
            producer_epoch: producer_epoch.to_string(),
            backend_process_instance: None,
            sequence: timing.sequence,
            observed_at_ms: Some(timing.observed_at_ms),
            last_success_at_ms: Some(timing.last_success_at_ms),
            availability,
            activity,
            connection: WorkspaceSampleConnection {
                state: WorkspaceConnectionState::Connected,
                freshness: WorkspaceFreshnessState::Fresh,
                route: Some("paired_mcp".to_string()),
                diagnostic: diagnostic.map(str::to_string),
            },
            capabilities: WorkspaceNativeCapabilities {
                exact_thread_read: WorkspaceCapabilityState::Supported,
                activity_detail: WorkspaceCapabilityState::Supported,
                configured_selection: WorkspaceCapabilityState::Supported,
                wakeup_delivery: WorkspaceCapabilityState::Supported,
                per_call_identity: WorkspaceCapabilityState::Supported,
            },
            configured_selection: selection,
        }
    }

    fn sample_choice() -> SelectedRoleChoice {
        SelectedRoleChoice {
            matched_difficulty: Some(Difficulty::Medium),
            tool_id: "tool-opencode".to_string(),
            adapter: ToolAdapter::OpencodeV2,
            route: RoleExecutionRoute::HeadlessWorker,
            model_id: "model-flash".to_string(),
            model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: Some("max".to_string()),
        }
    }

    fn sample_registry_snapshot(revision: u64) -> RegistrySnapshot {
        RegistrySnapshot {
            schema_version: 1,
            revision,
            tools: vec![ToolDefinition {
                id: "tool-opencode".to_string(),
                label: "OpenCode v2".to_string(),
                adapter: ToolAdapter::OpencodeV2,
                executable_path: Some(PathBuf::from("/usr/bin/opencode")),
                enabled: true,
            }],
            models: vec![ModelDefinition {
                id: "model-flash".to_string(),
                tool_id: "tool-opencode".to_string(),
                label: "DeepSeek v4.1 Flash".to_string(),
                model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                efforts: vec!["max".to_string()],
                allow_no_effort: false,
            }],
            profiles: vec![WorkflowProfile {
                decision_advisor: None,
                id: "profile-main".to_string(),
                name: "Main Workspace Profile".to_string(),
                assignments: vec![
                    RoleAssignment {
                        role: WorkflowRole::Orchestrator,
                        difficulty: Some(Difficulty::Medium),
                        choice: ModelChoice {
                            model_id: "model-flash".to_string(),
                            effort: Some("max".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Medium),
                        choice: ModelChoice {
                            model_id: "model-flash".to_string(),
                            effort: Some("max".to_string()),
                        },
                    },
                ],
            }],
            default_profile_id: Some("profile-main".to_string()),
        }
    }

    fn sample_project_base(project_dir: &Path) -> CanonicalProjectBaseRef {
        CanonicalProjectBaseRef {
            repository_path: path_text(project_dir),
            base_commit: "1111111111111111111111111111111111111111".to_string(),
            base_tree_sha: "2222222222222222222222222222222222222222".to_string(),
        }
    }

    fn make_coworker_job(
        job_id: &str,
        request_key: &str,
        binding_id: &str,
        thread_uuid: &str,
        project_dir: &Path,
        brief_summary: &str,
        phase: CoworkerJobPhase,
    ) -> CoworkerJobRecord {
        let work_brief = WorkBrief {
            brief_id: format!("brf_{job_id}"),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            project_base: sample_project_base(project_dir),
            kind: WorkBriefKind::NewJob,
            summary: brief_summary.to_string(),
            confirmed_requirements: vec!["Preserve read-only workspace boundaries.".to_string()],
            proposed_additions: Vec::new(),
            assumptions: Vec::new(),
            open_questions: Vec::new(),
            accepted_architect_decisions: Vec::new(),
        };
        let work_brief_digest = work_brief.digest().expect("work_brief digest");
        CoworkerJobRecord {
            job_id: job_id.to_string(),
            job_revision: 1,
            create_request_key: request_key.to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            phase,
            work_brief,
            work_brief_digest,
            supersedes_job_id: None,
            planning_result: None,
            approval_linkage: None,
            tasks: Vec::new(),
            holds: Vec::new(),
            outbox: Vec::new(),
            combined_candidates: Vec::new(),
        }
    }

    fn write_coworker_ledger(plans_dir: &Path, ledger: &CoworkerBindingLedger) -> PathBuf {
        let store = CoworkerWorkflowStore::new(plans_dir).expect("open CoworkerWorkflowStore");
        let path = store
            .ledger_path_for_binding(&ledger.binding_id)
            .expect("ledger path");
        let bytes = serde_json::to_vec_pretty(ledger).expect("serialize ledger");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .expect("open ledger file");
        file.write_all(&bytes).expect("write ledger");
        file.sync_all().expect("sync ledger");
        drop(file);
        set_mode(&path, 0o600);
        path
    }

    fn write_legacy_approvals_ledger(stores: &TestStores, approvals: &[ApprovalRecord]) {
        let approvals_path = stores.authority.join(APPROVALS_FILE_NAME);
        for approval in approvals {
            verify_approval_record_integrity(approval, &approvals_path)
                .expect("legacy approval integrity");
        }
        let ledger = serde_json::json!({
            "schema_version": AUTHORITY_SCHEMA_VERSION,
            "approvals": approvals,
        });
        let bytes = serde_json::to_vec_pretty(&ledger).expect("serialize legacy approvals");
        fs::write(&approvals_path, bytes).expect("write legacy approvals");
        set_mode(&approvals_path, 0o600);
    }

    fn make_legacy_approval(
        published: &PlanRevision,
        binding_id: &str,
        thread_uuid: &str,
        project: &Path,
        approved_task_ids: &[String],
        created_at_ms: i64,
    ) -> ApprovalRecord {
        let approved_set: BTreeSet<&str> = approved_task_ids.iter().map(String::as_str).collect();
        let approved_tasks: Vec<ApprovedTaskRecord> = published
            .tasks
            .iter()
            .filter(|task| approved_set.contains(task.id.as_str()))
            .map(|task| ApprovedTaskRecord {
                task_id: task.id.clone(),
                title: task.title.clone(),
                instruction: task.instruction.clone(),
                instruction_digest: sha256_hex(task.instruction.as_bytes()),
                role: task.role,
                difficulty: task.difficulty,
                tool_id: "tool-opencode".to_string(),
                adapter: ToolAdapter::CodexCli,
                provider: None,
                canonical_executable: None,
                model_id: "model-flash".to_string(),
                model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                base_model: "deepseek-v4.1-flash".to_string(),
                effort: Some("max".to_string()),
                cli_model_arg: None,
                standalone: None,
                file_permissions: None,
                depends_on: task.depends_on.clone(),
                allowed_write_paths: task.allowed_write_paths.clone(),
                task_digest: sha256_hex(task.id.as_bytes()),
                profile_id: None,
                command_key: None,
            })
            .collect();
        assert_eq!(
            approved_tasks.len(),
            approved_task_ids.len(),
            "legacy fixture approval must reference only tasks in the published revision"
        );
        let mut approval = ApprovalRecord {
            approval_id: String::new(),
            subject_digest: String::new(),
            policy_version: AUTHORITY_POLICY_VERSION,
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            plan_id: published.plan_id.clone(),
            plan_revision: published.revision,
            profile_id: published.profile_id.clone(),
            registry_revision: published.profile_revision,
            plan_digest: sha256_hex(b"legacy workspace plan digest"),
            repository_path: project.to_path_buf(),
            base_commit: "1111111111111111111111111111111111111111".to_string(),
            approved_task_ids: approved_tasks
                .iter()
                .map(|task| task.task_id.clone())
                .collect(),
            approved_write_paths: vec!["src/lib.rs".to_string()],
            max_parallel: 1,
            budgets: Default::default(),
            tasks: approved_tasks,
            created_at_ms,
        };
        approval.subject_digest =
            compute_record_subject_digest(&approval).expect("legacy approval subject digest");
        approval.approval_id = derive_approval_id(&approval.subject_digest);
        approval
    }

    fn write_legacy_plan_and_approval_with_tasks(
        stores: &TestStores,
        binding_id: &str,
        thread_uuid: &str,
        project: &Path,
        plan_id: &str,
        plan_tasks: Vec<PlanTask>,
        approved_task_ids: &[String],
        approval_created_at_ms: i64,
    ) -> (PlanRevision, ApprovalRecord) {
        let plan_store = PlanStore::open(&stores.plans).expect("open legacy PlanStore");
        let draft = plan_store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: plan_id.to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 1,
                    summary: "Legacy plan-only workspace summary".to_string(),
                    tasks: plan_tasks,
                },
                1_700_000_300_000,
            )
            .expect("save legacy plan draft");
        let published = plan_store
            .change_at(
                draft.revision,
                PlanChange::Publish {
                    plan_id: draft.plan_id.clone(),
                    revision: draft.revision,
                },
                1_700_000_300_500,
            )
            .expect("publish legacy plan");
        drop(plan_store);

        let approval = make_legacy_approval(
            &published,
            binding_id,
            thread_uuid,
            project,
            approved_task_ids,
            approval_created_at_ms,
        );
        write_legacy_approvals_ledger(stores, std::slice::from_ref(&approval));
        (published, approval)
    }

    fn write_legacy_plan_and_approval(
        stores: &TestStores,
        binding_id: &str,
        thread_uuid: &str,
        project: &Path,
    ) -> (PlanRevision, ApprovalRecord) {
        write_legacy_plan_and_approval_with_tasks(
            stores,
            binding_id,
            thread_uuid,
            project,
            "plan_ws_legacy_01",
            vec![PlanTask {
                id: "task-legacy".to_string(),
                title: "Legacy task".to_string(),
                instruction: "Do not leak this legacy instruction".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Medium,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            }],
            &["task-legacy".to_string()],
            1_700_000_301_000,
        )
    }

    fn publish_legacy_plan_revision(
        stores: &TestStores,
        binding_id: &str,
        plan_id: &str,
        expected_revision: u64,
        summary: &str,
        plan_tasks: Vec<PlanTask>,
    ) -> PlanRevision {
        let plan_store = PlanStore::open(&stores.plans).expect("open legacy PlanStore");
        let draft = plan_store
            .change_at(
                expected_revision,
                PlanChange::SaveDraft {
                    plan_id: plan_id.to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 1,
                    summary: summary.to_string(),
                    tasks: plan_tasks,
                },
                1_700_000_302_000,
            )
            .expect("save newer legacy plan draft");
        let published = plan_store
            .change_at(
                draft.revision,
                PlanChange::Publish {
                    plan_id: draft.plan_id.clone(),
                    revision: draft.revision,
                },
                1_700_000_302_500,
            )
            .expect("publish newer legacy plan");
        drop(plan_store);
        published
    }

    fn legacy_plan_task(id: &str, title: &str) -> PlanTask {
        PlanTask {
            id: id.to_string(),
            title: title.to_string(),
            instruction: "Do not leak this legacy instruction".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Medium,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        }
    }

    fn legacy_observation_job(
        job_id: &str,
        state: ObservationJobState,
        approval_status: &str,
        execution_status: &str,
        tasks: Vec<ObservationTask>,
    ) -> ObservationJob {
        ObservationJob {
            source_id: "default".to_string(),
            id: job_id.to_string(),
            project_label: "Legacy fallback project".to_string(),
            project_path: None,
            title: "Legacy fallback job".to_string(),
            thread_uuid: None,
            thread_label: None,
            binding_id: Some("bnd_ws_legacy_fallback_01".to_string()),
            plan_id: Some(job_id.to_string()),
            plan_revision: Some(1),
            state,
            approval_status: approval_status.to_string(),
            execution_status: execution_status.to_string(),
            review_state: None,
            updated_at_ms: Some(1_700_000_300_000),
            active_call_count: 0,
            tasks,
            summary: None,
        }
    }

    fn legacy_observation_task(id: &str, title: &str, status: &str) -> ObservationTask {
        ObservationTask {
            id: id.to_string(),
            title: title.to_string(),
            status: status.to_string(),
            depends_on: Vec::new(),
        }
    }

    #[test]
    fn workspace_snapshot_is_readonly_preserves_sample_timestamps_and_projects_deterministic_safe_messages(
    ) {
        let tree = TestTree::new("readonly-invariants");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("workspace-repo");
        // Store-backed external resolution proves the repository path from the frozen project
        // base; it never invokes Git. A non-symlink marker is enough for path validation.
        fs::write(project.join(".git"), b"gitdir: disposable fixture\n")
            .expect("write disposable .git marker");
        set_mode(&project.join(".git"), 0o600);
        let source_id = stores.canonical_source_id();
        let binding_id = "bnd_ws_readonly_01";
        let thread_uuid = "0196ed00-0000-7000-8000-000000000101";

        let plan_store = PlanStore::open(&stores.plans).expect("open PlanStore for fixture setup");
        let draft = plan_store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan_ws_01".to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 1,
                    summary: "Plan özeti: salt okunur çalışma alanı denetimi".to_string(),
                    tasks: vec![PlanTask {
                        id: "task-1".to_string(),
                        title: "Verify snapshot".to_string(),
                        instruction: "DO_NOT_LEAK_PROVIDER_INSTRUCTION_SENTINEL".to_string(),
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Medium,
                        depends_on: Vec::new(),
                        allowed_write_paths: vec!["src/lib.rs".to_string()],
                    }],
                },
                1_700_000_100_000,
            )
            .expect("save draft");
        let published = plan_store
            .change_at(
                1,
                PlanChange::Publish {
                    plan_id: "plan_ws_01".to_string(),
                    revision: draft.revision,
                },
                1_700_000_100_500,
            )
            .expect("publish plan");
        drop(plan_store);
        // Remove `plans.lock` left by fixture setup so we prove the reader NEVER recreates it.
        let plans_lock = stores.plans.join("plans.lock");
        if plans_lock.exists() {
            fs::remove_file(&plans_lock).expect("remove fixture plans.lock");
        }

        let compiled_plan = compile_plan_revision(
            &published,
            &sample_registry_snapshot(published.profile_revision),
        )
        .expect("compile published plan revision");

        let turkish_brief = "Türkçe özet: güvenli çalışma alanı görünümü doğrulanmalı";
        let mut job = make_coworker_job(
            "job_ws_01",
            "req_ws_01",
            binding_id,
            thread_uuid,
            &project,
            turkish_brief,
            CoworkerJobPhase::AwaitingApproval,
        );

        let orchestrator_choice = sample_choice();
        let planning_policy = ExecutionPolicy {
            schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
            policy_id: "pol_plan_ws_01".to_string(),
            allow_rules: vec![PolicyAllowRule {
                tool_id: orchestrator_choice.tool_id.clone(),
                model_id: orchestrator_choice.model_id.clone(),
                efforts: vec!["max".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Architect, WorkflowRole::Orchestrator],
            }],
        };
        let policy_digest = planning_policy.digest().expect("planning policy digest");
        let output_digest = sha256_hex(b"ws-readonly-orchestrator-output-v1");
        let reservation = PlanningCallReservation {
            reservation_id: "prs_ws_01".to_string(),
            request_key: "req_plan_ws_01".to_string(),
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Medium),
            selected_choice: orchestrator_choice.clone(),
            context_digest: sha256_hex(b"ws-readonly-orchestrator-context-v1"),
            call_project_base: Some(job.work_brief.project_base.clone()),
            status: PlanningReservationStatus::Completed,
            reserved_at_ms: 1_700_000_100_100,
            updated_at_ms: 1_700_000_101_000,
            run_id: Some("run_plan_ws_01".to_string()),
            report_artifact_id: Some("art_plan_ws_01".to_string()),
            output_digest: Some(output_digest.clone()),
            diagnostic: None,
        };
        let mut grant = PlanningAdviceGrant {
            grant_id: String::new(),
            create_request_key: "req_grant_ws_01".to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            project_base: job.work_brief.project_base.clone(),
            profile_id: published.profile_id.clone(),
            registry_revision: published.profile_revision,
            policy: planning_policy,
            policy_digest: policy_digest.clone(),
            capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
            budget: PlanningCallBudget {
                max_total_calls: 4,
                max_orchestrator_calls: 2,
                max_architect_calls: 2,
                max_timeout_ms_per_call: 30_000,
                max_output_bytes_per_call: 65_536,
            },
            usage: PlanningGrantUsage {
                reserved_total_calls: 1,
                completed_calls: 1,
                failed_calls: 0,
                unknown_effect_calls: 0,
                refused_before_effect_calls: 0,
                reserved_orchestrator_calls: 1,
                reserved_architect_calls: 0,
            },
            status: PlanningGrantStatus::Active,
            created_at_ms: 1_700_000_099_000,
            expires_at_ms: 1_800_000_000_000,
            revoked_at_ms: None,
            revoked_reason: None,
            operator_attested: true,
            native_per_call_identity_verified: false,
            scoped_profile_dependency_proof: None,
            reservations: vec![reservation.clone()],
        };
        grant.grant_id = grant.compute_expected_grant_id();
        grant.validate().expect("valid planning grant");

        job.planning_result = Some(PlanningResultRef {
            grant_id: grant.grant_id.clone(),
            reservation_id: reservation.reservation_id.clone(),
            request_key: reservation.request_key.clone(),
            run_id: "run_plan_ws_01".to_string(),
            report_artifact_id: "art_plan_ws_01".to_string(),
            output_digest,
            work_brief_digest: job.work_brief_digest.clone(),
            project_base: job.work_brief.project_base.clone(),
            profile_id: published.profile_id.clone(),
            registry_revision: published.profile_revision,
            policy_digest,
            selected_choice: orchestrator_choice,
            plan_id: published.plan_id.clone(),
            plan_revision: published.revision,
            plan_digest: compiled_plan.plan_digest,
            plan_status: published.status,
            task_interfaces: BTreeMap::new(),
            architect_advice_ids: Vec::new(),
            resolved_open_questions: Vec::new(),
            deferred_proposals: Vec::new(),
            recorded_at_ms: 1_700_000_101_000,
        });
        job.holds.push(WorkflowHoldRecord {
            hold_id: "hld_ws_01".to_string(),
            request_key: "req_hold_ws_01".to_string(),
            reason: WorkflowHoldReason::ConversationClarificationPending,
            detail: "Kullanıcı onayı bekleniyor.".to_string(),
            directly_affected_task_ids: Vec::new(),
            affected_write_paths: Vec::new(),
            affected_interfaces: Vec::new(),
            holds_entire_job: true,
            active: true,
            created_at_job_revision: 1,
            resolved_at_job_revision: None,
            resolution_note: None,
        });
        let ledger = CoworkerBindingLedger {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id: binding_id.to_string(),
            thread_uuid: Some(thread_uuid.to_string()),
            ledger_revision: 1,
            planning_grants: vec![grant],
            architect_advice: Vec::new(),
            jobs: vec![job],
            admission_markers: BTreeMap::new(),
            external_resolutions: Vec::new(),
        };
        ledger.validate(binding_id).expect("valid ledger");
        write_coworker_ledger(&stores.plans, &ledger);

        let now_ms = current_observation_time_ms();
        let sample_obs_ms = now_ms - 1_500;
        let sample_succ_ms = now_ms - 1_600;
        let chat_identity =
            WorkspaceChatIdentity::new(&source_id, binding_id, thread_uuid, &path_text(&project))
                .expect("valid chat identity");
        let sample = make_native_sample(
            chat_identity.clone(),
            "epoch_ws_01",
            SampleTiming {
                sequence: 7,
                observed_at_ms: sample_obs_ms,
                last_success_at_ms: sample_succ_ms,
            },
            WorkspaceAvailabilityState::Available,
            WorkspaceActivityState::Idle,
            WorkspaceConfiguredSelection::verified(Some("gpt-6-astra"), Some("high")),
            Some("diagnostic_never_used_as_title"),
        );
        write_workspace_native_sample_sidecar(&stores.state, &sample)
            .expect("write native sample sidecar");
        // Remove writer lockfile from fixture setup to prove reader never creates lockfiles.
        let sample_lock = stores
            .state
            .join("native-chat-samples")
            .join("native-chat-samples.lock");
        if sample_lock.exists() {
            fs::remove_file(&sample_lock).expect("remove fixture sample lock");
        }

        let before = snapshot_tree_fingerprints(tree.path());

        let service = ObservationService::new(stores.config(&app_data));
        let snap1 = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot 1");
        let snap2 = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot 2");

        let after = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            before, after,
            "workspace_snapshot must never create directories/locks/WAL or mutate file timestamps"
        );
        assert!(!stores.plans.join("plans.lock").exists());
        assert!(!stores.authority.join("authority.lock").exists());
        assert!(!sample_lock.exists());

        assert_eq!(snap1.native_samples.len(), 1);
        assert_eq!(snap1.native_samples[0].observed_at_ms, Some(sample_obs_ms));
        assert_eq!(
            snap1.native_samples[0].last_success_at_ms,
            Some(sample_succ_ms)
        );
        assert_eq!(snap2.native_samples[0].observed_at_ms, Some(sample_obs_ms));
        assert_eq!(
            snap2.native_samples[0].last_success_at_ms,
            Some(sample_succ_ms)
        );
        assert!(snap2.generated_at_ms >= snap1.generated_at_ms);

        // Native live chat state (`Idle`) remains independent from job attention (`NeedsUser`).
        assert_eq!(snap1.chats.len(), 1);
        assert_eq!(snap1.chats[0].identity, chat_identity);
        assert_eq!(
            snap1.chats[0].availability,
            WorkspaceAvailabilityState::Available
        );
        assert_eq!(snap1.chats[0].activity, WorkspaceActivityState::Idle);
        assert_eq!(snap1.chats[0].active_job_count, 1);
        assert_eq!(snap1.chats[0].attention_count, 1);
        assert!(
            !snap1.chats[0]
                .configured_selection
                .counts_as_runtime_telemetry
        );
        assert_eq!(snap1.chats[0].thread_label, "Codex chat");

        // Safe Messages: deterministic item_ids across refreshes, preserves Turkish text, never leaks instructions.
        assert!(!snap1.messages.requires_transcript);
        assert!(!snap1.messages.allows_synthetic_user_messages);
        assert_eq!(snap1.messages, snap2.messages);
        assert!(snap1.messages.items.iter().any(|m| m.source_kind
            == WorkspaceMessageSourceKind::WorkBriefSummary
            && m.summary == turkish_brief
            && m.scope.chat_identity.as_ref() == Some(&chat_identity)));
        assert!(snap1
            .messages
            .items
            .iter()
            .any(|m| m.source_kind == WorkspaceMessageSourceKind::PlanSummary
                && m.summary == "Plan özeti: salt okunur çalışma alanı denetimi"));
        assert!(
            snap1.messages.items.iter().all(|m| !m
                .summary
                .contains("DO_NOT_LEAK_PROVIDER_INSTRUCTION_SENTINEL")),
            "task instruction prompts must never enter Messages"
        );

        // Record a real external resolution receipt through the store-backed service, then verify
        // workspace, ObservationJob, ChatJobStats, and Messages all consume the same result.
        let external_service = ExternalResolutionService::new(
            &stores.plans,
            &stores.authority,
            &stores.registry,
            Some(&stores.sessions),
        )
        .expect("open external resolution service");
        let request = RecordExternalResolutionRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            canonical_repo_root: project.clone(),
            target: ExternalResolutionTarget::Job {
                job_id: "job_ws_01".to_string(),
            },
            scope: ExternalResolutionScope::FullJob,
            expected_ledger_revision: Some(1),
            expected_job_revision: Some(1),
            expected_runtime_revision: None,
            request_key: "req_ws_external_01".to_string(),
            created_at_ms: 1_700_000_200_000,
            explanation: "Resolved externally by the workspace fixture operator.".to_string(),
            evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
                reference: "issue-ws-42".to_string(),
            }],
        };
        let preview = external_service
            .preview(&request)
            .expect("preview external resolution");
        let receipt = external_service
            .record(
                &request,
                &ExternalResolutionConfirmation {
                    trusted_local_attested: true,
                    thread_uuid: preview.thread_uuid.clone(),
                    preview_digest: preview.preview_digest.clone(),
                },
            )
            .expect("record external resolution");
        let after_record = snapshot_tree_fingerprints(tree.path());
        let snap_ext = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot after external resolution");
        let after_read = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            after_record, after_read,
            "store-backed workspace reads must not write after a real receipt record"
        );

        let resolved_history = snap_ext
            .job_histories
            .iter()
            .find(|history| history.job.job_id == "job_ws_01")
            .expect("resolved job history");
        assert_eq!(resolved_history.external_resolutions.len(), 1);
        let resolution = &resolved_history.external_resolutions[0];
        assert_eq!(resolution.receipt_id, receipt.receipt_id);
        assert_eq!(
            resolution.applicability,
            threadlane_core::observation::WorkspaceExternalResolutionApplicability::Applicable
        );
        assert_eq!(
            resolution.scope,
            threadlane_core::observation::WorkspaceExternalResolutionScope::Job
        );
        assert_eq!(
            resolution.outcome,
            threadlane_core::observation::WorkspaceExternalResolutionOutcome::ResolvedExternally
        );
        assert!(!resolution.covered_attention.is_empty());
        assert!(resolved_history.current_attention.is_none());
        assert_eq!(
            resolved_history.job.phase.as_deref(),
            Some("awaiting_approval"),
            "stored phase remains unchanged"
        );
        assert_eq!(
            resolved_history.job.state,
            ObservationJobState::Unknown,
            "external resolution is a logical outcome, not Completed"
        );
        assert_eq!(resolved_history.job.execution_status, "resolved_externally");
        assert_eq!(
            resolved_history.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );
        assert_eq!(snap_ext.chats[0].attention_count, 0);
        assert!(snap_ext.observation.jobs.iter().any(|job| {
            job.id == "job_ws_01"
                && job.state == ObservationJobState::Unknown
                && job.execution_status == "resolved_externally"
        }));
        assert!(snap_ext.messages.items.iter().any(|message| {
            message.source_record_id == receipt.receipt_id
                && message.scope.job_id.as_deref() == Some("job_ws_01")
                && message.summary == resolution.summary
        }));
        assert!(
            snap_ext
                .messages
                .items
                .iter()
                .any(|message| message.summary == turkish_brief),
            "unrelated safe Messages rows stay visible after external resolution"
        );
    }

    #[test]
    fn workspace_snapshot_includes_verified_jobless_native_chat_and_preserves_stored_label() {
        let tree = TestTree::new("jobless-chat");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project_labeled = tree.dir("repo-alpha");
        let project_fallback = tree.dir("repo-beta");
        let source_id = stores.canonical_source_id();

        let binding_labeled = "bnd_jobless_labeled";
        let thread_labeled = "0196ed00-0000-7000-8000-000000000201";
        let mut core = Core::open(stores.state.join("state.db")).expect("open Core");
        let created = core
            .create_binding(thread_labeled, "Stored Native Thread Title")
            .expect("create binding");
        drop(core);

        let now_ms = current_observation_time_ms();
        let id_labeled = WorkspaceChatIdentity::new(
            &source_id,
            &created.binding_id,
            thread_labeled,
            &path_text(&project_labeled),
        )
        .expect("chat identity 1");
        let id_fallback = WorkspaceChatIdentity::new(
            &source_id,
            binding_labeled,
            "0196ed00-0000-7000-8000-000000000202",
            &path_text(&project_fallback),
        )
        .expect("chat identity 2");

        write_workspace_native_sample_sidecar(
            &stores.state,
            &make_native_sample(
                id_labeled.clone(),
                "epoch_jl_1",
                SampleTiming {
                    sequence: 1,
                    observed_at_ms: now_ms - 500,
                    last_success_at_ms: now_ms - 500,
                },
                WorkspaceAvailabilityState::Available,
                WorkspaceActivityState::Working,
                WorkspaceConfiguredSelection::verified(Some("gpt-6-astra"), Some("high")),
                Some("Diagnostic text must not become title"),
            ),
        )
        .expect("write sample 1");

        write_workspace_native_sample_sidecar(
            &stores.state,
            &make_native_sample(
                id_fallback.clone(),
                "epoch_jl_2",
                SampleTiming {
                    sequence: 1,
                    observed_at_ms: now_ms - 400,
                    last_success_at_ms: now_ms - 400,
                },
                WorkspaceAvailabilityState::Disconnected,
                WorkspaceActivityState::Working,
                WorkspaceConfiguredSelection::verified(Some("gpt-6-astra"), Some("high")),
                Some("Disconnected socket diagnostic"),
            ),
        )
        .expect("write sample 2");

        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot");

        assert!(snap.job_histories.is_empty());
        assert_eq!(snap.chats.len(), 2);

        let chat_labeled = snap
            .chats
            .iter()
            .find(|c| c.identity == id_labeled)
            .expect("labeled jobless chat");
        assert_eq!(chat_labeled.thread_label, "Stored Native Thread Title");
        assert_eq!(chat_labeled.project_label, "repo-alpha");
        assert_eq!(
            chat_labeled.availability,
            WorkspaceAvailabilityState::Available
        );
        assert_eq!(chat_labeled.activity, WorkspaceActivityState::Working);
        assert_eq!(chat_labeled.active_job_count, 0);
        assert_eq!(chat_labeled.attention_count, 0);
        assert_eq!(
            chat_labeled.configured_selection.evidence,
            WorkspaceSelectionEvidence::VerifiedConfiguration
        );
        assert!(
            !chat_labeled
                .configured_selection
                .counts_as_runtime_telemetry
        );

        let chat_fallback = snap
            .chats
            .iter()
            .find(|c| c.identity == id_fallback)
            .expect("fallback jobless chat");
        assert_eq!(chat_fallback.thread_label, "Codex chat");
        assert_eq!(
            chat_fallback.availability,
            WorkspaceAvailabilityState::Disconnected
        );
        assert_eq!(chat_fallback.activity, WorkspaceActivityState::Unknown);
        assert_eq!(
            chat_fallback.configured_selection.evidence,
            WorkspaceSelectionEvidence::Unavailable
        );
    }

    #[test]
    fn workspace_snapshot_isolates_healthy_source_from_missing_or_degraded_sources() {
        let tree = TestTree::new("multi-source-isolation");
        let app_data = tree.dir("app-data");
        let missing_default = tree.join("missing-default");

        let stores_healthy = TestStores::new(&tree, "healthy");
        let stores_degraded = TestStores::new(&tree, "degraded");
        let stores_missing = TestStores::new(&tree, "missing-ctx");

        let repo_healthy = tree.dir("repo-healthy");
        let repo_degraded = tree.dir("repo-degraded");
        let repo_missing = tree.dir("repo-missing");
        tree.dir("contexts");

        let ctx_healthy = tree.write_json(
            "contexts/healthy.json",
            &stores_healthy.context_json(
                &repo_healthy,
                "bnd_healthy",
                "0196ed00-0000-7000-8000-000000000301",
            ),
        );
        let ctx_degraded = tree.write_json(
            "contexts/degraded.json",
            &stores_degraded.context_json(
                &repo_degraded,
                "bnd_degraded",
                "0196ed00-0000-7000-8000-000000000302",
            ),
        );
        let ctx_missing = tree.write_json(
            "contexts/missing.json",
            &stores_missing.context_json(
                &repo_missing,
                "bnd_missing",
                "0196ed00-0000-7000-8000-000000000303",
            ),
        );

        let service = ObservationService::new(ObservationSourceConfig {
            runtime_state_dir: None,
            registry_dir: missing_default.join("registry"),
            plans_dir: missing_default.join("plans"),
            authority_dir: missing_default.join("authority"),
            sessions_dir: missing_default.join("sessions"),
            app_data_dir: app_data,
        });

        let src_healthy = service.link_context(&ctx_healthy).expect("link healthy");
        let src_degraded = service.link_context(&ctx_degraded).expect("link degraded");
        let src_missing = service.link_context(&ctx_missing).expect("link missing");
        assert_eq!(src_healthy.id, stores_healthy.canonical_source_id());
        assert_eq!(src_degraded.id, stores_degraded.canonical_source_id());
        assert_eq!(src_missing.id, stores_missing.canonical_source_id());

        // Populate healthy source with a valid job and fresh native sample.
        let healthy_job = make_coworker_job(
            "job_healthy_01",
            "req_healthy_01",
            "bnd_healthy",
            "0196ed00-0000-7000-8000-000000000301",
            &repo_healthy,
            "Healthy source work brief",
            CoworkerJobPhase::Planning,
        );
        let healthy_ledger = CoworkerBindingLedger {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id: "bnd_healthy".to_string(),
            thread_uuid: Some("0196ed00-0000-7000-8000-000000000301".to_string()),
            ledger_revision: 1,
            planning_grants: Vec::new(),
            architect_advice: Vec::new(),
            jobs: vec![healthy_job],
            admission_markers: BTreeMap::new(),
            external_resolutions: Vec::new(),
        };
        write_coworker_ledger(&stores_healthy.plans, &healthy_ledger);

        let now_ms = current_observation_time_ms();
        let healthy_chat = WorkspaceChatIdentity::new(
            &src_healthy.id,
            "bnd_healthy",
            "0196ed00-0000-7000-8000-000000000301",
            &path_text(&repo_healthy),
        )
        .expect("healthy chat id");
        write_workspace_native_sample_sidecar(
            &stores_healthy.state,
            &make_native_sample(
                healthy_chat.clone(),
                "epoch_h_1",
                SampleTiming {
                    sequence: 2,
                    observed_at_ms: now_ms - 300,
                    last_success_at_ms: now_ms - 300,
                },
                WorkspaceAvailabilityState::Available,
                WorkspaceActivityState::Idle,
                WorkspaceConfiguredSelection::unavailable(),
                None,
            ),
        )
        .expect("write healthy sample");

        // Corrupt `stores_degraded` ledger and sample sidecar, and delete `ctx_missing`.
        tree.write_file(
            "degraded/plans/coworker-workflow-00000000000000000000000000000000.json",
            b"{corrupt_json",
            0o600,
        );
        tree.dir("degraded/state/native-chat-samples");
        tree.write_file(
            "degraded/state/native-chat-samples/sample-0000000000000000000000000000000000000000000000000000000000000000.json",
            b"{bad_sample",
            0o600,
        );
        fs::remove_file(&ctx_missing).expect("remove missing context");

        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot");

        let missing_src = snap
            .observation
            .sources
            .iter()
            .find(|s| s.id == src_missing.id)
            .expect("missing source listed");
        assert_eq!(
            missing_src.availability,
            ObservationSourceAvailability::Unavailable
        );
        assert!(!snap.observation.warnings.is_empty());

        // Healthy source surfaces remain completely intact.
        assert!(snap.chats.iter().any(|c| c.identity == healthy_chat));
        assert!(snap
            .native_samples
            .iter()
            .any(|s| s.chat_identity == healthy_chat));
        assert!(snap
            .job_histories
            .iter()
            .any(|h| h.job.source_id == src_healthy.id && h.job.job_id == "job_healthy_01"));
        assert!(snap.messages.items.iter().any(
            |m| m.scope.source_id == src_healthy.id && m.summary == "Healthy source work brief"
        ));
    }

    #[test]
    fn workspace_snapshot_rejects_foreign_scope_and_never_synthesizes_identity_from_partial_hints()
    {
        let tree = TestTree::new("wrong-scope");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let real_repo = tree.dir("real-repo");
        let symlink_repo = tree.join("symlink-repo");
        symlink(&real_repo, &symlink_repo).expect("create symlink repo");
        let source_id = stores.canonical_source_id();

        // 1. Write a native sample belonging to a FOREIGN `source_id` into `stores.state`.
        let foreign_chat = WorkspaceChatIdentity::new(
            "src_ffffffffffffffffffffffffffffffff",
            "bnd_foreign",
            "0196ed00-0000-7000-8000-000000000401",
            &path_text(&real_repo),
        )
        .expect("foreign chat id");
        let now_ms = current_observation_time_ms();
        write_workspace_native_sample_sidecar(
            &stores.state,
            &make_native_sample(
                foreign_chat,
                "epoch_for_1",
                SampleTiming {
                    sequence: 1,
                    observed_at_ms: now_ms - 200,
                    last_success_at_ms: now_ms - 200,
                },
                WorkspaceAvailabilityState::Available,
                WorkspaceActivityState::Idle,
                WorkspaceConfiguredSelection::unavailable(),
                None,
            ),
        )
        .expect("write foreign sample");

        // 2. Write a coworker ledger where a job has a mismatched foreign `thread_uuid` and a
        //    second job is built canonically with `real_repo` then tampered ONLY at the raw
        //    stored-ledger JSON boundary to carry a symlink `repository_path`.
        let job_wrong_thread = make_coworker_job(
            "job_wrong_thread",
            "req_wrong_thread",
            "bnd_scope_test",
            "0196ed00-0000-7000-8000-000000000499",
            &real_repo,
            "Mismatched thread job",
            CoworkerJobPhase::Planning,
        );
        let job_symlink_repo = make_coworker_job(
            "job_symlink_repo",
            "req_symlink_repo",
            "bnd_scope_test",
            "0196ed00-0000-7000-8000-000000000402",
            &real_repo,
            "Symlink repo job",
            CoworkerJobPhase::Planning,
        );
        job_symlink_repo.validate().expect("valid canonical job");
        let base_ledger = CoworkerBindingLedger {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id: "bnd_scope_test".to_string(),
            thread_uuid: Some("0196ed00-0000-7000-8000-000000000402".to_string()),
            ledger_revision: 1,
            planning_grants: Vec::new(),
            architect_advice: Vec::new(),
            jobs: vec![job_wrong_thread, job_symlink_repo],
            admission_markers: BTreeMap::new(),
            external_resolutions: Vec::new(),
        };
        let ledger_path = write_coworker_ledger(&stores.plans, &base_ledger);
        let mut raw_ledger = serde_json::to_value(&base_ledger).expect("ledger json");
        let real_repo_text = path_text(&real_repo);
        assert_eq!(
            raw_ledger["jobs"][1]["work_brief"]["project_base"]["repository_path"].as_str(),
            Some(real_repo_text.as_str())
        );
        raw_ledger["jobs"][1]["work_brief"]["project_base"]["repository_path"] =
            serde_json::Value::String(path_text(&symlink_repo));
        let tampered_bytes = serde_json::to_vec_pretty(&raw_ledger).expect("json bytes");
        fs::write(&ledger_path, &tampered_bytes).expect("write tampered ledger");
        set_mode(&ledger_path, 0o600);

        // 3. Create an unbound legacy draft plan (has `binding_id` and title, but NO `thread_uuid` or `project_path`).
        let plan_store = PlanStore::open(&stores.plans).expect("open PlanStore");
        plan_store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan_unbound_draft".to_string(),
                    binding_id: "bnd_unbound_only".to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 1,
                    summary: "Unbound draft summary".to_string(),
                    tasks: vec![PlanTask {
                        id: "t1".to_string(),
                        title: "Draft task".to_string(),
                        instruction: "Hidden instruction".to_string(),
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Easy,
                        depends_on: Vec::new(),
                        allowed_write_paths: vec!["src/lib.rs".to_string()],
                    }],
                },
                1_700_000_200_000,
            )
            .expect("save unbound draft");
        drop(plan_store);

        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot");

        // Foreign sample, wrong-thread job,symlink-repo job, and unbound draft must produce ZERO chats.
        assert!(
            snap.chats.is_empty(),
            "expected no verified chats, got {:?}",
            snap.chats
        );
        assert!(snap.native_samples.is_empty());
        assert!(
            snap.job_histories
                .iter()
                .all(|h| h.job.job_id != "job_wrong_thread" && h.job.job_id != "job_symlink_repo"),
            "tampered coworker jobs must not enter workspace job_histories"
        );

        let draft_hist = snap
            .job_histories
            .iter()
            .find(|h| h.job.source_id == source_id && h.job.job_id == "plan_unbound_draft")
            .expect("legacy unbound draft history");
        assert!(
            draft_hist.job.chat_identity.is_none(),
            "must never synthesize chat_identity from source_id, binding_id alone, title, or job_id"
        );

        let draft_msg = snap
            .messages
            .items
            .iter()
            .find(|m| m.source_record_id == "plan_unbound_draft_r1")
            .expect("draft plan summary message");
        assert!(draft_msg.scope.chat_identity.is_none());
        assert!(draft_msg
            .completeness
            .missing_fields
            .contains(&"thread_uuid".to_string()));
        assert!(draft_msg
            .completeness
            .missing_fields
            .contains(&"canonical_project_scope".to_string()));
    }

    #[test]
    fn workspace_snapshot_reconciles_epoch_and_sequence_and_evaluates_staleness() {
        let tree = TestTree::new("epoch-and-staleness");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let repo_live = tree.dir("repo-live");
        let repo_stale = tree.dir("repo-stale");
        let source_id = stores.canonical_source_id();

        let now_ms = current_observation_time_ms();
        let live_chat = WorkspaceChatIdentity::new(
            &source_id,
            "bnd_seq_live",
            "0196ed00-0000-7000-8000-000000000501",
            &path_text(&repo_live),
        )
        .expect("live chat");
        let stale_chat = WorkspaceChatIdentity::new(
            &source_id,
            "bnd_seq_stale",
            "0196ed00-0000-7000-8000-000000000502",
            &path_text(&repo_stale),
        )
        .expect("stale chat");

        let write_out = write_workspace_native_sample_sidecar(
            &stores.state,
            &make_native_sample(
                live_chat.clone(),
                "epoch_01",
                SampleTiming {
                    sequence: 5,
                    observed_at_ms: now_ms - 500,
                    last_success_at_ms: now_ms - 500,
                },
                WorkspaceAvailabilityState::Available,
                WorkspaceActivityState::Working,
                WorkspaceConfiguredSelection::verified(Some("gpt-6-astra"), Some("high")),
                None,
            ),
        )
        .expect("write initial sample");

        let stale_ts = now_ms - 25_000;
        write_workspace_native_sample_sidecar(
            &stores.state,
            &make_native_sample(
                stale_chat.clone(),
                "epoch_stale_01",
                SampleTiming {
                    sequence: 2,
                    observed_at_ms: stale_ts,
                    last_success_at_ms: stale_ts,
                },
                WorkspaceAvailabilityState::Available,
                WorkspaceActivityState::Working,
                WorkspaceConfiguredSelection::verified(Some("gpt-6-astra"), Some("high")),
                None,
            ),
        )
        .expect("write stale sample");

        let service = ObservationService::new(stores.config(&app_data));
        let snap1 = service.workspace_snapshot(None).expect("snap1");

        let stale_summary = snap1
            .chats
            .iter()
            .find(|c| c.identity == stale_chat)
            .expect("stale chat summary");
        assert_eq!(
            stale_summary.availability,
            WorkspaceAvailabilityState::Stale
        );
        assert_eq!(stale_summary.activity, WorkspaceActivityState::Unknown);
        assert_eq!(
            stale_summary.configured_selection.evidence,
            WorkspaceSelectionEvidence::Unavailable
        );
        let stale_raw = snap1
            .native_samples
            .iter()
            .find(|s| s.chat_identity == stale_chat)
            .expect("stale raw sample");
        assert_eq!(stale_raw.observed_at_ms, Some(stale_ts));
        assert_eq!(stale_raw.last_success_at_ms, Some(stale_ts));

        // Overwrite the sidecar directly on disk with a regressed sequence (`seq = 3 < 5`) in `epoch_01`.
        let regressed = make_native_sample(
            live_chat.clone(),
            "epoch_01",
            SampleTiming {
                sequence: 3,
                observed_at_ms: now_ms - 400,
                last_success_at_ms: now_ms - 400,
            },
            WorkspaceAvailabilityState::Available,
            WorkspaceActivityState::Idle,
            WorkspaceConfiguredSelection::unavailable(),
            None,
        );
        let regressed_bytes =
            serialize_workspace_native_sample_sidecar(&regressed).expect("serialize regressed");
        fs::write(&write_out.path, &regressed_bytes).expect("overwrite sidecar with stale seq");
        set_mode(&write_out.path, 0o600);

        let snap2 = service.workspace_snapshot(None).expect("snap2");
        let retained_live = snap2
            .native_samples
            .iter()
            .find(|s| s.chat_identity == live_chat)
            .expect("retained sample");
        assert_eq!(
            retained_live.sequence, 5,
            "regressed sequence within same producer_epoch must be rejected"
        );
        let live_summary_2 = snap2
            .chats
            .iter()
            .find(|c| c.identity == live_chat)
            .expect("live summary 2");
        assert_eq!(live_summary_2.activity, WorkspaceActivityState::Working);
        assert!(snap2
            .observation
            .warnings
            .iter()
            .any(|w| w.contains("regressed native chat sample")));

        // Rotate `producer_epoch` to `epoch_02` with sequence reset to `1` and non-older `observed_at_ms`.
        let new_epoch_sample = make_native_sample(
            live_chat.clone(),
            "epoch_02",
            SampleTiming {
                sequence: 1,
                observed_at_ms: now_ms - 100,
                last_success_at_ms: now_ms - 100,
            },
            WorkspaceAvailabilityState::Available,
            WorkspaceActivityState::WaitingForInput,
            WorkspaceConfiguredSelection::verified(Some("gpt-6-astra"), Some("high")),
            None,
        );
        write_workspace_native_sample_sidecar(&stores.state, &new_epoch_sample)
            .expect("write rotated epoch sample");

        let snap3 = service.workspace_snapshot(None).expect("snap3");
        let rotated_live = snap3
            .native_samples
            .iter()
            .find(|s| s.chat_identity == live_chat)
            .expect("rotated sample");
        assert_eq!(rotated_live.producer_epoch, "epoch_02");
        assert_eq!(rotated_live.sequence, 1);
        let live_summary_3 = snap3
            .chats
            .iter()
            .find(|c| c.identity == live_chat)
            .expect("live summary 3");
        assert_eq!(
            live_summary_3.activity,
            WorkspaceActivityState::WaitingForInput
        );
    }

    #[test]
    fn workspace_snapshot_never_fabricates_acceptance_from_unproved_receipt_ids_and_supports_job_filter(
    ) {
        let tree = TestTree::new("non-fabricated-acceptance");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let repo = tree.dir("repo-closure");
        let source_id = stores.canonical_source_id();
        let binding_id = "bnd_unproved_closure";
        let thread_uuid = "0196ed00-0000-7000-8000-000000000601";

        let mut job_unproved = make_coworker_job(
            "job_unproved_01",
            "req_unproved_01",
            binding_id,
            thread_uuid,
            &repo,
            "Claims completion without receipt stores",
            CoworkerJobPhase::Completed,
        );
        job_unproved.approval_linkage = Some(FrozenApprovalLinkage {
            approval_id: "apr_unproved_01".to_string(),
            approval_subject_digest:
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
            plan_id: "plan_unproved_01".to_string(),
            plan_revision: 1,
            plan_digest: "9999999999999999999999999999999999999999999999999999999999999999"
                .to_string(),
            project_base: sample_project_base(&repo),
            profile_id: "prof-test".to_string(),
            registry_revision: 1,
            policy_id: "pol-test".to_string(),
            policy_digest: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                .to_string(),
            approved_task_ids: vec!["task-1".to_string()],
            approved_write_paths: vec!["src/lib.rs".to_string()],
            controller_envelope: None,
        });
        job_unproved.tasks = vec![CoworkerTaskRecord {
            task_id: "task-1".to_string(),
            task_digest: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                .to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Medium,
            selected_choice: sample_choice(),
            depends_on: Vec::new(),
            write_paths: vec!["src/lib.rs".to_string()],
            owned_interfaces: Vec::new(),
            consumed_interfaces: Vec::new(),
            stage: CoworkerTaskStage::Integrated,
            stage_before_hold: None,
            current_attempt_no: 1,
            active_candidate_id: Some("cnd_task-1".to_string()),
            accepted_candidate_id: Some("cnd_task-1".to_string()),
            dependency_input_commit: None,
            dependency_input_tree_sha: None,
            accepted_predecessor_receipts: Vec::new(),
            attempts: Vec::new(),
            candidates: vec![TaskCandidateRef {
                candidate_id: "cnd_task-1".to_string(),
                task_id: "task-1".to_string(),
                attempt_no: 1,
                base_commit: "1111111111111111111111111111111111111111".to_string(),
                head_commit: "3333333333333333333333333333333333333333".to_string(),
                tree_sha: "4444444444444444444444444444444444444444".to_string(),
                diff_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
                write_paths: vec!["src/lib.rs".to_string()],
                check_evidence_digests: vec![
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                ],
                review_subject_digest: Some(
                    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
                ),
                review_verdict: Some(StructuredReviewVerdict {
                    verdict_id: "vrd_task-1".to_string(),
                    task_id: "task-1".to_string(),
                    attempt_no: 1,
                    candidate_id: "cnd_task-1".to_string(),
                    base_commit: "1111111111111111111111111111111111111111".to_string(),
                    task_head_commit: "3333333333333333333333333333333333333333".to_string(),
                    task_tree_sha: "4444444444444444444444444444444444444444".to_string(),
                    diff_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                    review_subject_digest:
                        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                            .to_string(),
                    policy_digest:
                        "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                            .to_string(),
                    outcome: ReviewVerdictOutcome::Accept,
                    findings: Vec::new(),
                    required_in_scope_fixes: Vec::new(),
                    conflicting_paths_or_interfaces: Vec::new(),
                    prose_summary: Some("Looks good in prose only.".to_string()),
                    prose_may_advance_acceptance: false,
                }),
                review_receipt_id: Some("rev_plausible_fake_01".to_string()),
                integration_receipt_id: Some("int_plausible_fake_01".to_string()),
            }],
            active_hold_ids: Vec::new(),
            unresolved_outbox_id: None,
        }];

        let mut combined = CombinedCandidateIdentity {
            combined_candidate_id: "cmb_unproved_01".to_string(),
            request_key: "req_cmb_unproved_01".to_string(),
            job_id: "job_unproved_01".to_string(),
            job_revision: 1,
            project_base: sample_project_base(&repo),
            final_commit_sha: Some("5555555555555555555555555555555555555555".to_string()),
            final_tree_sha: "6666666666666666666666666666666666666666".to_string(),
            plan_id: "plan_unproved_01".to_string(),
            plan_revision: 1,
            plan_digest: "9999999999999999999999999999999999999999999999999999999999999999"
                .to_string(),
            approval_id: "apr_unproved_01".to_string(),
            approval_subject_digest:
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
            scope_policy_revision: 1,
            policy_id: "pol-test".to_string(),
            policy_digest: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                .to_string(),
            accepted_predecessor_receipts: vec![AcceptedPredecessorReceiptRef {
                task_id: "task-1".to_string(),
                candidate_id: "cnd_task-1".to_string(),
                head_commit: "3333333333333333333333333333333333333333".to_string(),
                tree_sha: "4444444444444444444444444444444444444444".to_string(),
                review_subject_digest:
                    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
                review_receipt_id: "rev_plausible_fake_01".to_string(),
                integration_receipt_id: "int_plausible_fake_01".to_string(),
            }],
            check_evidence: vec![CandidateCheckEvidence {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test".to_string(),
                passed: true,
                exit_code: 0,
                evidence_digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            }],
            candidate_digest: String::new(),
            closure_reservation_id: Some("cls_plausible_fake_01".to_string()),
            status: CombinedCandidateStatus::ClosureAccepted,
        };
        combined.candidate_digest = combined.compute_candidate_digest().expect("cmb digest");
        job_unproved.combined_candidates.push(combined);

        let job_planning = make_coworker_job(
            "job_sibling_planning",
            "req_sibling_planning",
            binding_id,
            thread_uuid,
            &repo,
            "Sibling planning brief",
            CoworkerJobPhase::Planning,
        );

        let ledger = CoworkerBindingLedger {
            schema_version: COWORKER_WORKFLOW_SCHEMA_VERSION,
            binding_id: binding_id.to_string(),
            thread_uuid: Some(thread_uuid.to_string()),
            ledger_revision: 1,
            planning_grants: Vec::new(),
            architect_advice: Vec::new(),
            jobs: vec![job_unproved, job_planning],
            admission_markers: BTreeMap::new(),
            external_resolutions: Vec::new(),
        };
        ledger.validate(binding_id).expect("valid ledger");
        write_coworker_ledger(&stores.plans, &ledger);

        // Prove the fixture authority directory starts empty, then remove it so the initial
        // observation scenario exercises an actually absent authority store rather than an
        // existing empty store disproving `apr_unproved_01`.
        assert!(
            fs::read_dir(&stores.authority)
                .expect("read fixture authority dir")
                .next()
                .is_none(),
            "fixture authority dir must start empty before removal"
        );
        fs::remove_dir(&stores.authority).expect("remove empty fixture authority dir");
        assert!(!stores.authority.exists());

        let chat_identity =
            WorkspaceChatIdentity::new(&source_id, binding_id, thread_uuid, &path_text(&repo))
                .expect("valid chat identity");
        let history_stores = WorkspaceHistoryStores::new(&stores.plans)
            .with_authority_dir(&stores.authority)
            .with_sessions_dir(&stores.sessions);

        let missing_res = read_scoped_workspace_job_histories(&chat_identity, &history_stores);
        let missing_auth_read = missing_res.expect("scoped read with absent authority");
        assert!(!stores.authority.exists());
        assert!(missing_auth_read.degraded);
        assert_eq!(missing_auth_read.rejected_scope_count, 0);
        assert!(missing_auth_read
            .diagnostics
            .iter()
            .any(|d| d.kind == WorkspaceHistoryDiagnosticKind::MissingStore
                && d.message.contains("authority store directory is missing")));
        assert!(missing_auth_read.diagnostics.iter().any(|d| d.kind
            == WorkspaceHistoryDiagnosticKind::AcceptanceVerificationFailed
            && d.job_id.as_deref() == Some("job_unproved_01")));

        let unproved_hist = missing_auth_read
            .jobs
            .iter()
            .find(|h| h.job.job_id == "job_unproved_01")
            .expect("unproved job history");
        assert_eq!(
            unproved_hist.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown,
            "plausible receipt IDs without backing closure/receipt stores must never verify as accepted"
        );
        assert_ne!(unproved_hist.job.state, ObservationJobState::Completed);
        assert_eq!(
            unproved_hist.tasks[0].acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );

        let service = ObservationService::new(stores.config(&app_data));
        let snap_absent_auth = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot with absent authority");
        assert!(!stores.authority.exists());
        let absent_src = snap_absent_auth
            .observation
            .sources
            .iter()
            .find(|s| s.id == source_id)
            .expect("default source listed when authority store is absent");
        assert_eq!(
            absent_src.availability,
            ObservationSourceAvailability::Unavailable
        );
        assert!(!snap_absent_auth.observation.warnings.is_empty());
        assert!(
            snap_absent_auth
                .messages
                .items
                .iter()
                .all(|m| m.source_kind != WorkspaceMessageSourceKind::ClosureSummary),
            "unverified closure must never emit ClosureSummary"
        );

        // Recreate a valid empty 0700 authority directory and read again: now the present
        // store disproves the nonexistent `apr_unproved_01`, omitting `job_unproved_01`.
        let recreated_authority = tree.dir("stores/authority");
        assert_eq!(recreated_authority, stores.authority);
        assert!(fs::read_dir(&stores.authority)
            .expect("read recreated authority dir")
            .next()
            .is_none());

        let empty_res = read_scoped_workspace_job_histories(&chat_identity, &history_stores);
        let empty_auth_read = empty_res.expect("scoped read with empty authority");
        assert_eq!(empty_auth_read.rejected_scope_count, 1);
        assert!(
            empty_auth_read
                .jobs
                .iter()
                .all(|h| h.job.job_id != "job_unproved_01"),
            "job with nonexistent approval must be omitted when authority store is empty"
        );
        assert!(empty_auth_read.diagnostics.iter().any(|d| d.kind
            == WorkspaceHistoryDiagnosticKind::ApprovalVerificationFailed
            && d.job_id.as_deref() == Some("job_unproved_01")));

        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot");
        assert!(
            snap.job_histories
                .iter()
                .all(|h| h.job.job_id != "job_unproved_01"),
            "workspace_snapshot must omit job with nonexistent approval in empty authority store"
        );
        assert!(snap
            .job_histories
            .iter()
            .any(|h| h.job.job_id == "job_sibling_planning"));
        assert!(snap
            .observation
            .warnings
            .iter()
            .any(|w| w.contains("job_unproved_01") && w.contains("approval_id was not found")));
        assert!(
            snap.messages
                .items
                .iter()
                .all(|m| m.source_kind != WorkspaceMessageSourceKind::ClosureSummary),
            "unverified closure must never emit ClosureSummary"
        );

        // Filtered request scopes `job_histories` and `messages` to the requested job.
        let filtered = service
            .workspace_snapshot(Some(ObservationSnapshotRequest {
                source_id: Some(source_id),
                job_id: Some("job_sibling_planning".to_string()),
                cursor: None,
                limit: Some(25),
            }))
            .expect("filtered workspace_snapshot");
        assert_eq!(filtered.job_histories.len(), 1);
        assert_eq!(filtered.job_histories[0].job.job_id, "job_sibling_planning");
        assert!(filtered
            .messages
            .items
            .iter()
            .all(|m| m.scope.job_id.as_deref() == Some("job_sibling_planning")));

        // `job_id` without `source_id` is rejected identically to legacy `snapshot`.
        let invalid_err = service
            .workspace_snapshot(Some(ObservationSnapshotRequest {
                source_id: None,
                job_id: Some("job_sibling_planning".to_string()),
                cursor: None,
                limit: None,
            }))
            .expect_err("job_id without source_id must fail");
        assert_eq!(invalid_err.code, "invalid_input");
    }

    #[test]
    fn workspace_snapshot_legacy_draft_and_unapproved_plans_stay_informational_planning() {
        let tree = TestTree::new("legacy-informational-planning");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let binding_id = "bnd_ws_legacy_info_01";

        let plan_store = PlanStore::open(&stores.plans).expect("open legacy informational plans");
        plan_store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan_ws_legacy_info_draft".to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 1,
                    summary: "Legacy informational draft summary".to_string(),
                    tasks: vec![legacy_plan_task("task-legacy-draft", "Legacy draft task")],
                },
                1_700_000_320_000,
            )
            .expect("save informational legacy draft");
        let unapproved = plan_store
            .change_at(
                0,
                PlanChange::SaveDraft {
                    plan_id: "plan_ws_legacy_info_unapproved".to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: 1,
                    summary: "Legacy informational unapproved summary".to_string(),
                    tasks: vec![legacy_plan_task(
                        "task-legacy-unapproved",
                        "Legacy unapproved task",
                    )],
                },
                1_700_000_321_000,
            )
            .expect("save informational legacy unapproved plan");
        plan_store
            .change_at(
                unapproved.revision,
                PlanChange::Publish {
                    plan_id: unapproved.plan_id.clone(),
                    revision: unapproved.revision,
                },
                1_700_000_321_500,
            )
            .expect("publish informational legacy unapproved plan");
        drop(plan_store);

        let before_read = snapshot_tree_fingerprints(tree.path());
        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot with informational legacy plans");
        let after_read = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            before_read, after_read,
            "informational legacy fallback reads must not write files"
        );

        for (job_id, expected_approval, task_id) in [
            ("plan_ws_legacy_info_draft", "draft", "task-legacy-draft"),
            (
                "plan_ws_legacy_info_unapproved",
                "unapproved",
                "task-legacy-unapproved",
            ),
        ] {
            let history = snap
                .job_histories
                .iter()
                .find(|history| history.job.job_id == job_id)
                .unwrap_or_else(|| panic!("missing informational legacy history {job_id}"));
            assert_eq!(history.job.state, ObservationJobState::Planning);
            assert_eq!(history.job.approval_status, expected_approval);
            assert_eq!(history.job.execution_status, "not_started");
            assert!(
                history.external_resolutions.is_empty(),
                "the informational fixture has no external resolution receipt"
            );
            assert!(
                history.current_attention.is_none(),
                "unapproved/not_started job {job_id} must not invent current attention"
            );
            assert_eq!(history.tasks.len(), 1);
            let task = &history.tasks[0];
            assert_eq!(task.task_id, task_id);
            assert_eq!(task.status, "not_approved");
            assert!(
                task.current_attention.is_none(),
                "unapproved task {task_id} must remain informational"
            );
            assert_eq!(
                task.acceptance_proof.status,
                WorkspaceAcceptanceStatus::Unknown,
                "informational planning must not claim acceptance"
            );
        }
        assert!(
            snap.chats.is_empty(),
            "unapproved plan-only fixtures have no verified chat identity"
        );
    }

    #[test]
    fn workspace_legacy_fallback_keeps_unknown_observation_informational() {
        let job = legacy_observation_job(
            "job_legacy_unknown_information",
            ObservationJobState::Unknown,
            "unknown",
            "unknown",
            vec![
                legacy_observation_task("task-legacy-unknown", "Legacy unknown task", "unknown"),
                legacy_observation_task(
                    "task-legacy-not-approved",
                    "Legacy not approved task",
                    "not_approved",
                ),
                legacy_observation_task(
                    "task-legacy-not-started",
                    "Legacy not started task",
                    "not_started",
                ),
            ],
        );

        let history = super::super::build_legacy_fallback_job_history(&job, None, None);
        assert_eq!(history.job.state, ObservationJobState::Unknown);
        assert_eq!(history.job.approval_status, "unknown");
        assert_eq!(history.job.execution_status, "unknown");
        assert!(history.job.chat_identity.is_none());
        assert!(
            history.current_attention.is_none(),
            "generic unknown job state must stay informational"
        );
        assert!(history.external_resolutions.is_empty());

        let unknown_task = &history.tasks[0];
        assert_eq!(unknown_task.task_id, "task-legacy-unknown");
        assert_eq!(unknown_task.status, "unknown");
        assert!(
            unknown_task.current_attention.is_none(),
            "unclassified unknown task evidence must not invent a human request"
        );
        assert_eq!(
            unknown_task.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );

        let not_approved_task = &history.tasks[1];
        assert_eq!(not_approved_task.task_id, "task-legacy-not-approved");
        assert_eq!(not_approved_task.status, "not_approved");
        assert!(not_approved_task.current_attention.is_none());
        assert_eq!(
            not_approved_task.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );

        let not_started_task = &history.tasks[2];
        assert_eq!(not_started_task.task_id, "task-legacy-not-started");
        assert_eq!(not_started_task.status, "not_started");
        assert!(not_started_task.current_attention.is_none());
        assert_eq!(
            not_started_task.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );
    }

    #[test]
    fn workspace_legacy_fallback_preserves_actionable_evidence_without_id_prose() {
        let opaque_ids = [
            "task-legacy-failed-opaque",
            "task-legacy-timeout-opaque",
            "task-legacy-review-opaque",
            "task-legacy-native-opaque",
            "task-legacy-not-approved-opaque",
            "task-legacy-unknown-opaque",
        ];
        let job = legacy_observation_job(
            "job_legacy_actionable_information",
            ObservationJobState::NeedsUser,
            "approved",
            "unknown",
            vec![
                legacy_observation_task(opaque_ids[0], "Legacy failed task", "failed"),
                legacy_observation_task(opaque_ids[1], "Legacy timed out task", "timed_out"),
                legacy_observation_task(opaque_ids[2], "Legacy review task", "ready_for_review"),
                legacy_observation_task(
                    opaque_ids[3],
                    "Legacy native task",
                    "native_route_required",
                ),
                legacy_observation_task(opaque_ids[4], "Legacy not approved task", "not_approved"),
                legacy_observation_task(opaque_ids[5], "Legacy unknown task", "unknown"),
            ],
        );

        let history = super::super::build_legacy_fallback_job_history(&job, None, None);
        assert!(
            history.current_attention.is_none(),
            "task-specific evidence must remain on the task rows"
        );
        for (task_id, expected_status, expected_kind) in [
            (
                opaque_ids[0],
                "failed",
                WorkspaceAttentionKind::BlockedFailure,
            ),
            (
                opaque_ids[1],
                "timed_out",
                WorkspaceAttentionKind::BlockedFailure,
            ),
            (
                opaque_ids[2],
                "ready_for_review",
                WorkspaceAttentionKind::ReviewRequired,
            ),
            (
                opaque_ids[3],
                "native_route_required",
                WorkspaceAttentionKind::NeedsUserInput,
            ),
        ] {
            let task = history
                .tasks
                .iter()
                .find(|task| task.task_id == task_id)
                .unwrap_or_else(|| panic!("missing actionable legacy task {task_id}"));
            assert_eq!(task.status, expected_status);
            let attention = task
                .current_attention
                .as_ref()
                .unwrap_or_else(|| panic!("missing actionable attention for {task_id}"));
            assert_eq!(attention.kind, expected_kind);
            assert!(
                !attention.reason.contains(task_id),
                "ordinary reason prose must not repeat opaque task id {task_id}"
            );
            for opaque_id in opaque_ids {
                assert!(
                    !attention.reason.contains(opaque_id),
                    "ordinary reason prose must not repeat any opaque task id"
                );
            }
        }
        for task_id in [opaque_ids[4], opaque_ids[5]] {
            let task = history
                .tasks
                .iter()
                .find(|task| task.task_id == task_id)
                .unwrap_or_else(|| panic!("missing informational legacy task {task_id}"));
            assert!(
                task.current_attention.is_none(),
                "informational task {task_id} must not invent attention"
            );
            assert_eq!(
                task.acceptance_proof.status,
                WorkspaceAcceptanceStatus::Unknown
            );
        }

        let needs_user_job = legacy_observation_job(
            "job_legacy_needs_user_fallback",
            ObservationJobState::NeedsUser,
            "approved",
            "not_started",
            vec![legacy_observation_task(
                "task-legacy-informational-fallback",
                "Legacy informational task",
                "not_approved",
            )],
        );
        let needs_user_history =
            super::super::build_legacy_fallback_job_history(&needs_user_job, None, None);
        let needs_user_attention = needs_user_history
            .current_attention
            .as_ref()
            .expect("explicit NeedsUser job state must keep its actionable fallback");
        assert!(!needs_user_attention
            .reason
            .contains("job_legacy_needs_user_fallback"));
        assert!(!needs_user_attention
            .reason
            .contains("task-legacy-informational-fallback"));
        assert!(
            needs_user_history
                .tasks
                .iter()
                .all(|task| task.current_attention.is_none()),
            "the NeedsUser fallback must not fabricate task-level attention"
        );

        let failed_job = legacy_observation_job(
            "job_legacy_failed_fallback",
            ObservationJobState::Failed,
            "approved",
            "failed",
            vec![legacy_observation_task(
                "task-legacy-informational-failed-job",
                "Legacy informational task",
                "not_approved",
            )],
        );
        let failed_history =
            super::super::build_legacy_fallback_job_history(&failed_job, None, None);
        assert_eq!(
            failed_history
                .current_attention
                .as_ref()
                .map(|attention| attention.kind),
            Some(WorkspaceAttentionKind::BlockedFailure)
        );
    }

    #[test]
    fn workspace_snapshot_projects_real_legacy_plan_external_resolution_without_coworker_job() {
        let tree = TestTree::new("legacy-external-resolution");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("legacy-project");
        fs::write(project.join(".git"), b"gitdir: disposable fixture\n")
            .expect("write disposable legacy .git marker");
        set_mode(&project.join(".git"), 0o600);
        let binding_id = "bnd_ws_legacy_external_01";
        let thread_uuid = "0196ed00-0000-7000-8000-000000000701";
        let (plan, _approval) =
            write_legacy_plan_and_approval(&stores, binding_id, thread_uuid, &project);

        let external_service = ExternalResolutionService::new(
            &stores.plans,
            &stores.authority,
            &stores.registry,
            Some(&stores.sessions),
        )
        .expect("open legacy external resolution service");
        let request = RecordExternalResolutionRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            canonical_repo_root: project.clone(),
            target: ExternalResolutionTarget::LegacyPlan {
                plan_id: plan.plan_id.clone(),
                plan_revision: plan.revision,
            },
            scope: ExternalResolutionScope::FullJob,
            expected_ledger_revision: None,
            expected_job_revision: None,
            expected_runtime_revision: None,
            request_key: "req_ws_legacy_external_01".to_string(),
            created_at_ms: 1_700_000_400_000,
            explanation: "Legacy plan was resolved outside Threadlane.".to_string(),
            evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
                reference: "issue-legacy-ws-7".to_string(),
            }],
        };
        let preview = external_service
            .preview(&request)
            .expect("preview legacy external resolution");
        let receipt = external_service
            .record(
                &request,
                &ExternalResolutionConfirmation {
                    trusted_local_attested: true,
                    thread_uuid: preview.thread_uuid.clone(),
                    preview_digest: preview.preview_digest.clone(),
                },
            )
            .expect("record legacy external resolution");
        let before_read = snapshot_tree_fingerprints(tree.path());

        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot with legacy external resolution");
        let after_read = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            before_read, after_read,
            "legacy fallback history reads must not create locks, caches, or other files"
        );

        let history = snap
            .job_histories
            .iter()
            .find(|history| history.job.job_id == plan.plan_id)
            .expect("legacy fallback job history");
        assert_eq!(history.job.source_id, stores.canonical_source_id());
        assert_eq!(
            history.links.source_plan_id.as_deref(),
            Some(plan.plan_id.as_str())
        );
        assert_eq!(history.links.source_plan_revision, Some(plan.revision));
        assert_eq!(
            history
                .job
                .chat_identity
                .as_ref()
                .map(|id| id.binding_id.as_str()),
            Some(binding_id)
        );
        assert_eq!(history.external_resolutions.len(), 1);
        let resolution = &history.external_resolutions[0];
        assert_eq!(resolution.receipt_id, receipt.receipt_id);
        assert_eq!(
            resolution.applicability,
            WorkspaceExternalResolutionApplicability::Applicable
        );
        assert_eq!(resolution.scope, WorkspaceExternalResolutionScope::Job);
        assert!(history.current_attention.is_none());
        assert_eq!(history.job.state, ObservationJobState::Unknown);
        assert_eq!(history.job.execution_status, "resolved_externally");
        assert_eq!(
            history.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );
        assert_eq!(
            history.tasks[0].status, "native_route_required",
            "legacy fallback must preserve the original task outcome"
        );

        assert!(snap.observation.jobs.iter().any(|job| {
            job.id == plan.plan_id
                && job.state == ObservationJobState::Unknown
                && job.execution_status == "resolved_externally"
        }));
        assert!(snap.messages.items.iter().any(|message| {
            message.source_record_id == receipt.receipt_id
                && message.scope.job_id.as_deref() == Some(plan.plan_id.as_str())
                && message.summary == resolution.summary
        }));
        assert!(snap.messages.items.iter().any(|message| {
            message.source_kind == WorkspaceMessageSourceKind::PlanSummary
                && message.summary == "Legacy plan-only workspace summary"
        }));
    }

    #[test]
    fn workspace_snapshot_legacy_subset_full_receipt_preserves_unapproved_displayed_work() {
        let tree = TestTree::new("legacy-subset-resolution");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("legacy-subset-project");
        fs::write(project.join(".git"), b"gitdir: disposable fixture\n")
            .expect("write disposable legacy .git marker");
        set_mode(&project.join(".git"), 0o600);
        let binding_id = "bnd_ws_legacy_subset_01";
        let thread_uuid = "0196ed00-0000-7000-8000-000000000711";
        let alpha = "task-legacy-alpha";
        let beta = "task-legacy-beta";
        let (plan, _approval) = write_legacy_plan_and_approval_with_tasks(
            &stores,
            binding_id,
            thread_uuid,
            &project,
            "plan_ws_legacy_subset_01",
            vec![
                legacy_plan_task(alpha, "Legacy alpha task"),
                legacy_plan_task(beta, "Legacy beta task"),
            ],
            &[alpha.to_string()],
            1_700_000_311_000,
        );

        let external_service = ExternalResolutionService::new(
            &stores.plans,
            &stores.authority,
            &stores.registry,
            Some(&stores.sessions),
        )
        .expect("open legacy subset external resolution service");
        let request = RecordExternalResolutionRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            canonical_repo_root: project.clone(),
            target: ExternalResolutionTarget::LegacyPlan {
                plan_id: plan.plan_id.clone(),
                plan_revision: plan.revision,
            },
            scope: ExternalResolutionScope::FullJob,
            expected_ledger_revision: None,
            expected_job_revision: None,
            expected_runtime_revision: None,
            request_key: "req_ws_legacy_subset_01".to_string(),
            created_at_ms: 1_700_000_411_000,
            explanation: "The approved subset was resolved outside Threadlane.".to_string(),
            evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
                reference: "issue-legacy-ws-subset".to_string(),
            }],
        };
        let preview = external_service
            .preview(&request)
            .expect("preview legacy subset external resolution");
        let receipt = external_service
            .record(
                &request,
                &ExternalResolutionConfirmation {
                    trusted_local_attested: true,
                    thread_uuid: preview.thread_uuid.clone(),
                    preview_digest: preview.preview_digest.clone(),
                },
            )
            .expect("record legacy subset external resolution");
        let before_read = snapshot_tree_fingerprints(tree.path());

        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot with legacy subset external resolution");
        let after_read = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            before_read, after_read,
            "subset legacy fallback reads must not create locks, caches, or other files"
        );

        let history = snap
            .job_histories
            .iter()
            .find(|history| history.job.job_id == plan.plan_id)
            .expect("legacy subset fallback job history");
        assert_eq!(history.external_resolutions.len(), 1);
        let resolution = &history.external_resolutions[0];
        assert_eq!(resolution.receipt_id, receipt.receipt_id);
        assert_eq!(
            resolution.applicability,
            WorkspaceExternalResolutionApplicability::Applicable
        );
        assert_eq!(
            resolution.scope,
            WorkspaceExternalResolutionScope::Tasks,
            "a full receipt over a subset approval must project task-scoped workspace coverage"
        );
        assert_eq!(resolution.task_ids, vec![alpha.to_string()]);
        assert_eq!(resolution.covered_attention.len(), 1);
        assert_eq!(
            resolution.covered_attention[0].task_id.as_deref(),
            Some(alpha)
        );
        assert_eq!(
            resolution.covered_attention[0].attention.kind,
            WorkspaceAttentionKind::NeedsUserInput
        );
        assert!(
            !resolution.covered_attention[0]
                .attention
                .reason
                .contains(alpha),
            "covered attention prose must not repeat the typed task id"
        );

        let alpha_task = history
            .tasks
            .iter()
            .find(|task| task.task_id == alpha)
            .expect("legacy subset alpha task");
        let beta_task = history
            .tasks
            .iter()
            .find(|task| task.task_id == beta)
            .expect("legacy subset beta task");
        assert_eq!(alpha_task.status, "native_route_required");
        assert!(alpha_task.current_attention.is_none());
        assert_eq!(beta_task.status, "not_approved");
        assert!(
            beta_task.current_attention.is_none(),
            "the unapproved displayed task must remain informational and uncovered"
        );
        assert_eq!(
            beta_task.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown,
            "unapproved displayed work must not claim acceptance or resolution"
        );
        assert!(
            resolution
                .covered_attention
                .iter()
                .all(|covered| covered.task_id.as_deref() != Some(beta)),
            "the subset receipt must not overclaim the remaining unapproved task"
        );
        assert_eq!(history.job.state, ObservationJobState::NeedsUser);
        assert_ne!(history.job.execution_status, "resolved_externally");
        assert_eq!(
            history.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );
        assert_eq!(
            snap.chats
                .iter()
                .find(|chat| chat.identity.binding_id == binding_id)
                .map(|chat| (chat.active_job_count, chat.attention_count)),
            Some((1, 1)),
            "the remaining displayed work must keep the job active with attention"
        );
        assert!(snap.observation.jobs.iter().any(|job| {
            job.id == plan.plan_id
                && job.state == ObservationJobState::NeedsUser
                && job.execution_status != "resolved_externally"
        }));
        assert!(snap.messages.items.iter().any(|message| {
            message.source_record_id == receipt.receipt_id
                && message.scope.job_id.as_deref() == Some(plan.plan_id.as_str())
        }));
    }

    #[test]
    fn workspace_snapshot_legacy_exact_full_receipt_covers_details_and_retires_attention() {
        let tree = TestTree::new("legacy-exact-full-resolution");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("legacy-full-project");
        fs::write(project.join(".git"), b"gitdir: disposable fixture\n")
            .expect("write disposable legacy .git marker");
        set_mode(&project.join(".git"), 0o600);
        let binding_id = "bnd_ws_legacy_full_01";
        let thread_uuid = "0196ed00-0000-7000-8000-000000000712";
        let alpha = "task-legacy-alpha";
        let beta = "task-legacy-beta";
        let approved = vec![alpha.to_string(), beta.to_string()];
        let (plan, _approval) = write_legacy_plan_and_approval_with_tasks(
            &stores,
            binding_id,
            thread_uuid,
            &project,
            "plan_ws_legacy_full_01",
            vec![
                legacy_plan_task(alpha, "Legacy alpha task"),
                legacy_plan_task(beta, "Legacy beta task"),
            ],
            &approved,
            1_700_000_312_000,
        );

        let external_service = ExternalResolutionService::new(
            &stores.plans,
            &stores.authority,
            &stores.registry,
            Some(&stores.sessions),
        )
        .expect("open legacy full external resolution service");
        let request = RecordExternalResolutionRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            canonical_repo_root: project.clone(),
            target: ExternalResolutionTarget::LegacyPlan {
                plan_id: plan.plan_id.clone(),
                plan_revision: plan.revision,
            },
            scope: ExternalResolutionScope::FullJob,
            expected_ledger_revision: None,
            expected_job_revision: None,
            expected_runtime_revision: None,
            request_key: "req_ws_legacy_full_01".to_string(),
            created_at_ms: 1_700_000_412_000,
            explanation: "The complete displayed legacy plan was resolved outside Threadlane."
                .to_string(),
            evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
                reference: "issue-legacy-ws-full".to_string(),
            }],
        };
        let preview = external_service
            .preview(&request)
            .expect("preview legacy full external resolution");
        let receipt = external_service
            .record(
                &request,
                &ExternalResolutionConfirmation {
                    trusted_local_attested: true,
                    thread_uuid: preview.thread_uuid.clone(),
                    preview_digest: preview.preview_digest.clone(),
                },
            )
            .expect("record legacy full external resolution");
        let before_read = snapshot_tree_fingerprints(tree.path());

        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot with legacy full external resolution");
        let after_read = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            before_read, after_read,
            "exact full legacy fallback reads must not create locks, caches, or other files"
        );

        let history = snap
            .job_histories
            .iter()
            .find(|history| history.job.job_id == plan.plan_id)
            .expect("legacy full fallback job history");
        let resolution = &history.external_resolutions[0];
        assert_eq!(resolution.receipt_id, receipt.receipt_id);
        assert_eq!(
            resolution.applicability,
            WorkspaceExternalResolutionApplicability::Applicable
        );
        assert_eq!(resolution.scope, WorkspaceExternalResolutionScope::Job);
        assert_eq!(resolution.task_ids, approved);
        let covered_task_ids: BTreeSet<&str> = resolution
            .covered_attention
            .iter()
            .filter_map(|covered| covered.task_id.as_deref())
            .collect();
        assert_eq!(
            covered_task_ids,
            BTreeSet::from([alpha, beta]),
            "both prior task attention details must be preserved under the receipt"
        );
        assert!(resolution
            .covered_attention
            .iter()
            .all(|covered| { covered.attention.kind == WorkspaceAttentionKind::NeedsUserInput }));
        assert!(history.current_attention.is_none());
        assert!(history
            .tasks
            .iter()
            .all(|task| task.current_attention.is_none()));
        assert_eq!(history.job.state, ObservationJobState::Unknown);
        assert_eq!(history.job.execution_status, "resolved_externally");
        assert_eq!(
            history.acceptance_proof.status,
            WorkspaceAcceptanceStatus::Unknown
        );
        assert!(history
            .tasks
            .iter()
            .all(|task| task.status == "native_route_required"));
        assert_eq!(
            snap.chats
                .iter()
                .find(|chat| chat.identity.binding_id == binding_id)
                .map(|chat| (chat.active_job_count, chat.attention_count)),
            Some((0, 0))
        );
        assert!(snap.observation.jobs.iter().any(|job| {
            job.id == plan.plan_id
                && job.state == ObservationJobState::Unknown
                && job.execution_status == "resolved_externally"
        }));
        assert!(snap.messages.items.iter().any(|message| {
            message.source_record_id == receipt.receipt_id
                && message.scope.job_id.as_deref() == Some(plan.plan_id.as_str())
        }));
    }

    #[test]
    fn workspace_snapshot_legacy_partial_multi_receipts_keep_residual_job_attention() {
        let tree = TestTree::new("legacy-partial-multi-resolution");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("legacy-partial-project");
        fs::write(project.join(".git"), b"gitdir: disposable fixture\n")
            .expect("write disposable legacy .git marker");
        set_mode(&project.join(".git"), 0o600);
        let binding_id = "bnd_ws_legacy_partial_01";
        let thread_uuid = "0196ed00-0000-7000-8000-000000000713";
        let alpha = "task-legacy-alpha";
        let beta = "task-legacy-beta";
        let approved = vec![alpha.to_string(), beta.to_string()];
        let (plan, _approval) = write_legacy_plan_and_approval_with_tasks(
            &stores,
            binding_id,
            thread_uuid,
            &project,
            "plan_ws_legacy_partial_01",
            vec![
                legacy_plan_task(alpha, "Legacy alpha task"),
                legacy_plan_task(beta, "Legacy beta task"),
            ],
            &approved,
            1_700_000_313_000,
        );

        let external_service = ExternalResolutionService::new(
            &stores.plans,
            &stores.authority,
            &stores.registry,
            Some(&stores.sessions),
        )
        .expect("open legacy partial external resolution service");
        let record_task = |request_key: &str, task_id: &str, created_at_ms: u64| {
            let request = RecordExternalResolutionRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                canonical_repo_root: project.clone(),
                target: ExternalResolutionTarget::LegacyPlan {
                    plan_id: plan.plan_id.clone(),
                    plan_revision: plan.revision,
                },
                scope: ExternalResolutionScope::Tasks {
                    task_ids: vec![task_id.to_string()],
                },
                expected_ledger_revision: None,
                expected_job_revision: None,
                expected_runtime_revision: None,
                request_key: request_key.to_string(),
                created_at_ms,
                explanation: "An approved legacy task was resolved outside Threadlane.".to_string(),
                evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
                    reference: format!("issue-legacy-ws-{task_id}"),
                }],
            };
            let preview = external_service
                .preview(&request)
                .expect("preview partial legacy external resolution");
            external_service
                .record(
                    &request,
                    &ExternalResolutionConfirmation {
                        trusted_local_attested: true,
                        thread_uuid: preview.thread_uuid.clone(),
                        preview_digest: preview.preview_digest.clone(),
                    },
                )
                .expect("record partial legacy external resolution")
        };
        let alpha_receipt = record_task("req_ws_legacy_partial_alpha", alpha, 1_700_000_413_000);
        let beta_receipt = record_task("req_ws_legacy_partial_beta", beta, 1_700_000_414_000);
        let before_read = snapshot_tree_fingerprints(tree.path());

        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot with partial legacy external resolutions");
        let after_read = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            before_read, after_read,
            "partial multi-receipt legacy reads must not create locks, caches, or other files"
        );

        let history = snap
            .job_histories
            .iter()
            .find(|history| history.job.job_id == plan.plan_id)
            .expect("legacy partial fallback job history");
        assert_eq!(history.external_resolutions.len(), 2);
        assert!(history.external_resolutions.iter().all(|resolution| {
            resolution.applicability == WorkspaceExternalResolutionApplicability::Applicable
                && resolution.scope == WorkspaceExternalResolutionScope::Tasks
        }));
        assert_eq!(
            history
                .external_resolutions
                .iter()
                .map(|resolution| resolution.receipt_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                alpha_receipt.receipt_id.as_str(),
                beta_receipt.receipt_id.as_str()
            ])
        );
        for resolution in &history.external_resolutions {
            assert_eq!(resolution.covered_attention.len(), 1);
            assert!(resolution.covered_attention.iter().all(|covered| {
                covered.task_id.as_deref() == resolution.task_ids.first().map(String::as_str)
                    && covered.attention.kind == WorkspaceAttentionKind::NeedsUserInput
            }));
        }
        assert!(history
            .tasks
            .iter()
            .all(|task| task.current_attention.is_none()));
        assert_eq!(
            history
                .current_attention
                .as_ref()
                .map(|attention| attention.kind),
            Some(WorkspaceAttentionKind::Unknown),
            "unproven full displayed coverage must retain conservative residual job attention"
        );
        assert_eq!(history.job.state, ObservationJobState::NeedsUser);
        assert_ne!(history.job.execution_status, "resolved_externally");
        assert_eq!(
            snap.chats
                .iter()
                .find(|chat| chat.identity.binding_id == binding_id)
                .map(|chat| (chat.active_job_count, chat.attention_count)),
            Some((1, 1))
        );
        assert!(snap.observation.jobs.iter().any(|job| {
            job.id == plan.plan_id
                && job.state == ObservationJobState::NeedsUser
                && job.execution_status != "resolved_externally"
        }));
    }

    #[test]
    fn workspace_snapshot_legacy_old_revision_receipt_stays_stale_after_newer_published_revision() {
        let tree = TestTree::new("legacy-stale-revision-resolution");
        let stores = TestStores::new(&tree, "stores");
        let app_data = tree.dir("app-data");
        let project = tree.dir("legacy-stale-project");
        fs::write(project.join(".git"), b"gitdir: disposable fixture\n")
            .expect("write disposable legacy .git marker");
        set_mode(&project.join(".git"), 0o600);
        let binding_id = "bnd_ws_legacy_stale_01";
        let thread_uuid = "0196ed00-0000-7000-8000-000000000714";
        let alpha = "task-legacy-alpha";
        let beta = "task-legacy-beta";
        let gamma = "task-legacy-gamma";
        let (old_plan, old_approval) = write_legacy_plan_and_approval_with_tasks(
            &stores,
            binding_id,
            thread_uuid,
            &project,
            "plan_ws_legacy_stale_01",
            vec![
                legacy_plan_task(alpha, "Legacy alpha task"),
                legacy_plan_task(beta, "Legacy beta task"),
            ],
            &[alpha.to_string(), beta.to_string()],
            1_700_000_314_000,
        );

        let external_service = ExternalResolutionService::new(
            &stores.plans,
            &stores.authority,
            &stores.registry,
            Some(&stores.sessions),
        )
        .expect("open legacy stale external resolution service");
        let request = RecordExternalResolutionRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            canonical_repo_root: project.clone(),
            target: ExternalResolutionTarget::LegacyPlan {
                plan_id: old_plan.plan_id.clone(),
                plan_revision: old_plan.revision,
            },
            scope: ExternalResolutionScope::FullJob,
            expected_ledger_revision: None,
            expected_job_revision: None,
            expected_runtime_revision: None,
            request_key: "req_ws_legacy_stale_01".to_string(),
            created_at_ms: 1_700_000_415_000,
            explanation: "The original legacy revision was resolved outside Threadlane."
                .to_string(),
            evidence_refs: vec![ExternalResolutionEvidenceRef::ArtifactRef {
                reference: "issue-legacy-ws-stale".to_string(),
            }],
        };
        let preview = external_service
            .preview(&request)
            .expect("preview legacy stale external resolution");
        let receipt = external_service
            .record(
                &request,
                &ExternalResolutionConfirmation {
                    trusted_local_attested: true,
                    thread_uuid: preview.thread_uuid.clone(),
                    preview_digest: preview.preview_digest.clone(),
                },
            )
            .expect("record legacy stale external resolution");

        let newer = publish_legacy_plan_revision(
            &stores,
            binding_id,
            &old_plan.plan_id,
            old_plan.revision,
            "Legacy plan-only workspace summary revision two",
            vec![
                legacy_plan_task(alpha, "Legacy alpha task"),
                legacy_plan_task(gamma, "Legacy gamma task"),
            ],
        );
        let newer_approval = make_legacy_approval(
            &newer,
            binding_id,
            thread_uuid,
            &project,
            &[alpha.to_string(), gamma.to_string()],
            1_700_000_316_000,
        );
        write_legacy_approvals_ledger(&stores, &[old_approval, newer_approval]);
        let before_read = snapshot_tree_fingerprints(tree.path());

        let service = ObservationService::new(stores.config(&app_data));
        let snap = service
            .workspace_snapshot(None)
            .expect("workspace_snapshot with stale legacy external resolution");
        let after_read = snapshot_tree_fingerprints(tree.path());
        assert_eq!(
            before_read, after_read,
            "stale legacy history reads must not create locks, caches, or other files"
        );

        let history = snap
            .job_histories
            .iter()
            .find(|history| history.job.job_id == old_plan.plan_id)
            .expect("legacy stale fallback job history");
        assert_eq!(history.links.source_plan_revision, Some(newer.revision));
        assert_eq!(history.external_resolutions.len(), 1);
        let resolution = &history.external_resolutions[0];
        assert_eq!(resolution.receipt_id, receipt.receipt_id);
        assert_eq!(
            resolution.applicability,
            WorkspaceExternalResolutionApplicability::Stale
        );
        assert_eq!(resolution.scope, WorkspaceExternalResolutionScope::Job);
        assert_eq!(
            resolution.task_ids,
            vec![alpha.to_string(), beta.to_string()],
            "the immutable original receipt task IDs must remain untouched"
        );
        assert!(
            resolution.covered_attention.is_empty(),
            "a stale historical receipt must not cover the newer displayed revision"
        );
        let displayed_task_ids: BTreeSet<&str> = history
            .tasks
            .iter()
            .map(|task| task.task_id.as_str())
            .collect();
        assert_eq!(displayed_task_ids, BTreeSet::from([alpha, gamma]));
        assert!(history
            .tasks
            .iter()
            .all(|task| task.status == "native_route_required"));
        assert_eq!(
            history
                .current_attention
                .as_ref()
                .map(|attention| attention.kind),
            Some(WorkspaceAttentionKind::NeedsUserInput)
        );
        assert_eq!(history.job.state, ObservationJobState::NeedsUser);
        assert_ne!(history.job.execution_status, "resolved_externally");
        assert_eq!(
            snap.chats
                .iter()
                .find(|chat| chat.identity.binding_id == binding_id)
                .map(|chat| (chat.active_job_count, chat.attention_count)),
            Some((1, 1)),
            "the newer displayed revision keeps its active job and current attention"
        );
        assert!(snap.observation.jobs.iter().any(|job| {
            job.id == old_plan.plan_id
                && job.state == ObservationJobState::NeedsUser
                && job.execution_status != "resolved_externally"
        }));
        assert!(snap.messages.items.iter().any(|message| {
            message.source_record_id == receipt.receipt_id
                && message.scope.job_id.as_deref() == Some(old_plan.plan_id.as_str())
        }));
    }
}
