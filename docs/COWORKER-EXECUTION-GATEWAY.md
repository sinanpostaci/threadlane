# Coworker Execution Gateway (`threadlane-native-mcp` -> `CoworkerCodeDriver`)

This document describes the scoped execution handoff from native Coworker MCP (`PairedMcpServer` in `crates/threadlane-native/src/mcp_pair.rs`) to the independent `CoworkerCodeDriver` (`crates/threadlane-native/src/execution_gateway.rs` and `crates/threadlane-native/src/coworker_driver.rs`).

## Scope and Authority Invariants

1. **Authoritative Server-Injected Scope Only**:
   - Every execution tool requires `verify_enrolled_binding_for_tool` and configured workflow (`registry_dir`, `plans_dir`) and host (`authority_dir`, `sessions_dir`) stores on `PairedMcpServer`.
   - `binding_id`, `thread_uuid`, store directory paths, and the canonical same-package sibling `threadlane-flow` binary path are injected strictly by the server—never accepted from tool arguments.
   - All request and response types enforce `#[serde(deny_unknown_fields)]`. Caller-supplied rebind keys (`binding_id`, `thread_uuid`, `session_id`, `connection_nonce`, `epoch_id`, etc.) fail closed with `rebind_rejected`; extra model, tool, profile, or path fields fail closed with `validation_failed`.
2. **No Self-Approval, Grant Creation, Plan Publication, or Envelope Freeze from MCP**:
   - `CoworkerExecutionGateway` never creates or mutates `ApprovalRecord`, `FrozenApprovalLinkage`, `FrozenControllerCapabilityEnvelope`, or `FrozenControllerExecutionEnvelope`.
   - Human approval and execution envelope freezing remain explicit trusted-local CLI operations via `threadlane-flow` (`operator_attested: true`). Trusted-local CLI approval records local operator attestation on the host machine; it is not cryptographic proof of human identity (`cryptographic_human_identity_proven: false`, `native_per_call_identity_verified: false`).
   - Saved workflow profiles and model assignments never grant execution authority (`saved_profiles_grant_execution_authority: false`). Native Coworker model selection stays controlled by Codex.
   - Once the published plan is approved, bound to the job with a `FrozenControllerCapabilityEnvelope`, and frozen into an operator-attested `FrozenControllerExecutionEnvelope`, native Coworker MCP can run `preview_execution`, `start_execution`, `execution_status`, and `stop_execution` without an extra control-panel step.

## Advertised MCP Tools (`36 -> 40`)

### 1. `preview_execution { job_id }`
- **Pure read-only, process-free, provider-free**: Inspects `CoworkerWorkflowStore`, `ControllerPolicyStore`, and `AuthorityStore` without spawning processes, calling model providers, creating `sessions_dir` subdirectories, or acquiring physical job locks.
- **Concrete bounded scope**: Returns `approved: bool`, `readiness` (`needs_approval`, `missing_execution_envelope`, or `approved_and_frozen`), canonical `project_base` (`repository_path`, `base_commit`, `base_tree_sha`), `work_brief_id`/`work_brief_digest`, `plan_id`/`plan_revision`/`plan_digest`, `approval_id`/`approval_subject_digest`, `capability_envelope_digest`, `envelope_id`/`envelope_digest`, `max_parallel`, `approved_write_paths`, prompt-redacted `tasks`, `task_coder_choices`, `reviewer_choice`, `closure_choice`, `role_budgets`, correction/attempt caps, `allowed_check_ids`, path-redacted `check_commands` (basename `executable_name`, `argv`, `spec_digest`), and `pipeline_boundary`.
- **Redaction guarantees**: Never leaks raw task prompt instructions, secret tokens, full host executable paths (`FrozenCheckCommandSpec::executable`), registry tool executable paths, or private logs.

### 2. `start_execution { job_id, expected_envelope_digest }`
- Verifies that `job_id` belongs to the enrolled `(binding_id, thread_uuid)`, is not superseded, has a verified `ApprovalRecord`, `FrozenApprovalLinkage`, `FrozenControllerCapabilityEnvelope`, and operator-attested `FrozenControllerExecutionEnvelope`, and that `expected_envelope_digest` matches the frozen `envelope_digest` in constant time **before** opening `CoworkerCodeDriver` or spawning any process.
- Unapproved jobs return `unauthorized_operator` (`needs_approval`); jobs missing capability or execution envelopes return `unauthorized_operator` or `not_configured` (`missing_execution_envelope`); mismatched digests return `validation_failed` (`envelope_digest_mismatch`) with zero process spawns and zero provider invocations.
- Resolves and validates the canonical same-package sibling `threadlane-flow` binary and invokes `CoworkerCodeDriver::ensure_owner`.
- **Single physical owner & idempotent duplicate calls**: Spawns a detached `threadlane-flow ... code-driver run-owner` process (returning once the owner's local Unix socket RPC is verified, without blocking MCP until the entire job finishes) or reuses the existing verified live owner (`disposition: "reused_live_owner"`) without duplicating provider calls.

### 3. `execution_status { job_id }`
- Verifies scoped job/approval/envelope admission and delegates to `CoworkerCodeDriver::status`.
- Reports `owner_state` (`live_verified_owner`, `no_active_owner`, `stale_crash_artifact_detected`, etc.), `job_phase`, `overall_status`, `stop_requested`, `blocked_on_unknown_effect`, per-task stages and host observations, and `pipeline_boundary`.

### 4. `stop_execution { job_id, reason? }`
- Verifies scoped job/approval/envelope admission and delegates to `CoworkerCodeDriver::cancel`.
- Persists durable `stop_requested = true` in `ControllerPolicyStore` **before** notifying any live owner over authenticated local Unix socket RPC or attempting narrow `ApprovedHostClient::cancel_task` cancellation (`durable_stop_recorded_before_host_cancel: true`). Never performs broad PID or process-name kills.

## Current Staged Pipeline and Cancellation Boundaries

- **Pipeline boundary (`phase1_initial_dispatch_and_reconcile_only`)**: `CoworkerCodeDriver` currently runs Phase 1 initial `Coder` task dispatch and receipt reconciliation. Completed `Coder` runs transition to `ReadyForReview` (`overall_status: "awaiting_checks_or_review"`, `job_phase: "executing"`) and are never reported as accepted or integrated. Automatic verification check execution, `Reviewer` evaluation, in-scope corrections, per-task `RunIntegration`, and final job `Closure` are wired in separate independent branches.
- **Cancellation boundary (`original_host_tasks_only_derived_integrated_later`)**: `stop_execution` records durable job stop state first and cancels active original `ApprovedHostClient` tasks (`task_id -> run_id`); cancellation of running derived correction attempts is integrated with the full automatic pipeline milestone.
- **Unknown post-launch outcomes**: Tasks or effects in `EffectUnknown` (`blocked_on_unknown_effect: true`) block automatic replay and require receipt reconciliation rather than blind retry.
