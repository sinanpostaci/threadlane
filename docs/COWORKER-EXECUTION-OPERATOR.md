# Coworker Controller Execution Envelope Operator Guide

`threadlane-flow` provides the trusted-local operator CLI subcommand group `coworker-execution` (visible alias `execution-envelope`) to preview, record, and inspect frozen Coworker controller execution envelopes (`FrozenControllerExecutionEnvelope` and `ControllerJobRuntimeState`).

## Scope and Architectural Boundaries

- **Native Codex remains the human interface**: The human operator stays in native Codex; no separate desktop panel is required.
- **Model-less native Coworker**: The conversational Coworker is selected by the user in Codex, has no workflow profile model slot, and never borrows profile models. Headless roles (`Coder`, `Reviewer`, `Closure`, `Orchestrator`, `Architect`) must resolve to `RoleExecutionRoute::HeadlessWorker` and never `ToolAdapter::CodexCli` (`NativeHandoff`).
- **Configuration recording only**: `coworker-execution` validates and persists controller policy configuration in `plans_dir` (`coworker-controller-<tag>.json`, mode `0600`). It **never** starts or invokes any model, headless worker, check binary, or Git worktree/integration operation itself.
- **Downstream phases**: Automatic controller scheduling/driving and paired MCP exposure of controller execution envelopes belong to subsequent phases and are not yet active through this CLI.
- **Local attestation boundary**: `--operator-approved` records trusted-local operator attestation (`operator_attested: true`), not cryptographic proof of human intent or per-call chat provenance (`native_per_call_identity_verified` is always `false`). JSON fields, model output, or MCP prose can never self-approve.

## CLI Invocations and Flag Positions

`threadlane-flow` requires five existing canonical non-symlink `0700` store directories (`--state-dir`, `--registry-dir`, `--plans-dir`, `--authority-dir`, `--sessions-dir`) plus `--binding <ID>` and `--thread <UUID>`. Because these seven flags are declared `global = true` on `Cli`, `threadlane-flow` accepts them either before or after the subcommand and verifies `(binding, thread)` against the live local supervisor before touching any store.

When using a private (`0600`) v1 operator context file, invoke `threadlane-context --context <ABS_PRIVATE_JSON> flow coworker-execution ...`, which validates the context and forwards those exact seven locked store/scope flags to the canonical sibling `threadlane-flow` binary (`threadlane-flow` itself does not take `--context` directly):

```bash
# 1. Pure read-only preview via packaged operator context router
threadlane-context --context /abs/path/operator-context.json flow \
  coworker-execution preview \
  --request /abs/path/freeze-envelope-request.json

# 2. Record immutable previewed envelope (requires --operator-approved and BOTH digests)
threadlane-context --context /abs/path/operator-context.json flow \
  coworker-execution record \
  --request /abs/path/freeze-envelope-request.json \
  --confirm-approval <64_HEX_APPROVAL_SUBJECT_DIGEST> \
  --confirm-envelope <64_HEX_PREVIEW_ENVELOPE_DIGEST> \
  --operator-approved

# 3. Pure read-only inspection (shown with direct threadlane-flow global flags)
threadlane-flow \
  --state-dir /abs/path/runtime \
  --registry-dir /abs/path/registry-store \
  --plans-dir /abs/path/plans-store \
  --authority-dir /abs/path/authority-store \
  --sessions-dir /abs/path/sessions-store \
  --binding <BINDING_ID> \
  --thread <CANONICAL_LOWERCASE_THREAD_UUID> \
  coworker-execution inspect \
  --job-id <JOB_ID>
```

## Subcommand Semantics

1. **`coworker-execution preview --request <ABS>`**
   - Reads a private (`0600`, non-symlink, `<= 1 MiB`) `FreezeControllerExecutionEnvelopeRequest` JSON file.
   - Forces `operator_attested = false` and clears confirmation digests in memory regardless of JSON contents.
   - Cross-verifies the live `CoworkerJobRecord` (`expected_job_revision`), `ApprovalRecord`, active published `PlanRevision`, `RegistryStore` revision, canonical repository `project_base`, and headless `PreflightReport`.
   - Emits `ControllerConfigurationPreview` (`envelope_id`, `envelope_digest`, `approval_subject_digest`, `work_brief_digest`, `plan_digest`, `policy_digest`, `capability_envelope_digest`, `check_spec_digests`, `allow_check_failure_corrections` when enabled, and `envelope`) inside the bounded (`<= 1 MiB`) `trusted_local_operator` stdout envelope without creating or mutating any ledger, lockfile, approval record, or observation journal entry.

2. **`coworker-execution record --request <ABS> --confirm-approval <SHA256> --confirm-envelope <SHA256> --operator-approved`**
   - Visible flag aliases: `--confirm-approval-subject-digest`, `--confirm-envelope-digest`, and `--operator-attested`.
   - Rejects JSON self-attestation (`unauthorized_operator`) if `--operator-approved`, `--confirm-approval`, or `--confirm-envelope` is omitted on the command line, or if non-empty JSON confirmation fields disagree with the CLI flags.
   - Re-runs full live preview validation and verifies in constant time that `--confirm-approval` matches the job's frozen `approval_subject_digest` and `--confirm-envelope` matches the recomputed full `envelope_digest`. Any change to request inputs or live store revisions after preview changes the digest or fails validation, requiring a fresh `preview`.
   - Persists `ControllerJobRuntimeState` (`runtime_revision: 1`) and appends a `flow.coworker-execution.record` observation journal entry. Repeating an identical `record` request is idempotent; attempting to record a different envelope on the same job fails closed (`validation_failed`).

3. **`coworker-execution inspect --job-id <ID>`**
   - Validates `job_id` syntax, performs a lock-free read (`get_job_runtime_readonly`), verifies `state.envelope.verify_matches_job(&job)`, and returns `ControllerJobRuntimeState` (or `not_configured` if unrecorded) with zero filesystem, lock, ACK, or journal side effects.

## Required Request Fields (`FreezeControllerExecutionEnvelopeRequest`)

The `--request <ABS>` file must be a private (`0600`) JSON object (`#[serde(deny_unknown_fields)]`) containing:

- **Scope & Job CAS**: `binding_id: String`, `thread_uuid: String`, `job_id: String`, `expected_job_revision: u64`, `created_at_ms: u64`.
- **Policy & Scoped Headless Models**: `policy: ExecutionPolicy` (must match the job's frozen `policy_digest`), `reviewer_choice: SelectedRoleChoice`, and `closure_choice: SelectedRoleChoice` (each must be permitted by `policy` and listed in the job's `FrozenControllerCapabilityEnvelope`; per-task `Coder` choices are frozen directly from `job.tasks`).
- **Role & Attempt Caps**: `role_budgets: BTreeMap<WorkflowRole, ControllerRoleExecutionBudget>` (required entries for `coder`, `reviewer`, and `closure` bounding calls, runtime, timeout, stdout/stderr, and structured response bytes), `max_in_scope_corrections_per_task: u32` (`<= 2` and `<= capability_envelope.max_in_scope_correction_attempts`), `max_total_corrections_per_job: u32` (`<= 16`), and `max_total_task_attempts_per_job: u32` (`job.tasks.len()..=64`).
- **Frozen Check Commands**: `check_commands: BTreeMap<String, FrozenCheckCommandSpec>` (non-empty, each `check_id` authorized by `allowed_check_ids`, `kind: FrozenCheckKind` in `formatting | clippy | tests | custom`, canonical non-symlink `executable` rejecting shell/wrapper binaries such as `sh`/`bash`/`env`/`python`, bounded `argv` forbidding `-c`/`/C`/`--eval`, safe `repo_relative_cwd`, finite `timeout_ms`/`max_stdout_bytes`/`max_stderr_bytes`, and at least one check with `required_for_task_review: true` and one with `required_for_candidate_closure: true`).
- **Git Powers, Check-Failure Correction Capability & Optional Receipts**: `allow_task_worktree_commits: bool`, `allow_run_integration_execution: bool` (requires `local_git_integration_allowed: true` in the capability envelope), `default_integration_strategy: IntegrationStrategy` (`fast_forward_only` or `merge_no_ff`), optional `independence_completeness_receipt: Option<DeclaredIndependenceCompletenessReceipt>`, and optional `shadow_jev_policy: Option<ShadowJevAdvisorPolicy>` (`mode` must be `shadow`). Automatic failed-check correction preparation (`allow_check_failure_corrections`) is derived directly from the job's frozen `FrozenControllerCapabilityEnvelope` (`cap_env.allow_check_failure_corrections && max_in_scope_corrections_per_task > 0`), defaults to `false` (preserving byte-identical v1 digests when `false` and using a domain-separated v2 digest when `true`), and cannot be enabled by mutating an already-frozen approval or execution envelope.
- **Attestation Fields**: `operator_attested: bool` (`false` in preview requests), plus optional `confirm_approval_subject_digest: Option<String>` and `confirm_envelope_digest: Option<String>` (governed by CLI flags on `record`).

## Behavioral Test Coverage

- **`crates/threadlane-native/tests/coworker_execution_cli.rs` (5 integration tests)**:
  - `help_and_invalid_coworker_execution_cli_arguments_use_bounded_error_envelope`
  - `coworker_execution_preview_and_inspect_are_pure_and_return_exact_identity`
  - `coworker_execution_record_rejects_missing_attestation_or_confirm_digests_and_wrong_context`
  - `coworker_execution_preview_record_and_inspect_idempotent_lifecycle`
  - `coworker_execution_rejects_tampered_checks_caps_models_native_routes_and_stale_revisions`
- **`crates/threadlane-native/tests/coworker_grant_cli.rs` (4 grant CLI regression tests)**:
  - `help_and_invalid_grant_cli_arguments_use_bounded_error_envelope`
  - `grant_prepare_list_and_get_are_read_only_even_with_parent_observation_env`
  - `grant_record_rejects_file_only_attestation_digest_mismatch_and_wrong_scope`
  - `grant_record_list_get_and_revoke_lifecycle_with_cas_and_stale_registry_refusal`
- **`crates/threadlane-native/src/bin/threadlane-flow.rs` unit tests**: `cli_parses_coworker_execution_subcommands_with_global_flags` and `cli_parses_grant_subcommands_with_global_flags`.

### Approved Codex Coder tasks

A newly approved complete Codex CLI Coder contract can run through the same frozen controller
pipeline as the other supported CLI Coders. The trusted preview must include the canonical
executable, model/effort, TaskFiles scope and provider pins. Saved profiles are not execution
authority. Legacy `provider=None` approvals remain manual native routes. Missing runtime
model/effort telemetry stays unknown; exact verified launch evidence and its private source
digest are carried separately through checks, review receipts, dependencies and integration.
See [Codex Coder](CODEX-CODER.md) for the launch and qualification boundaries.
