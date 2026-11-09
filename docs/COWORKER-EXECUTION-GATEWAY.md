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
   - The supported new-job order is: obtain the exact authority approval for the published planning result; run trusted-local `flow coworker-admission preview`; present every role, model/effort/canonical pin, scope, budget, check, correction and Git power; record only the unchanged confirmed preview with `--operator-approved` and all three confirmation digests; inspect persistence and `preview_execution` readiness; separately call `start_execution` with the returned frozen digest. An identical already-approved complete scope requires no redundant consent. Worker-only approval does not imply downstream powers. See [approval admission](COWORKER-APPROVAL-ADMISSION.md) for private-context commands and wire examples.
   - Admission record/reconcile starts no execution. Immutable linkage/envelope conflicts require the supported new-job or amendment/replan/reapproval path; never edit an old job to add powers. Older valid manual binding/envelope routes remain available within their original scope.

## Advertised MCP Tools (`36 -> 40`)

### 1. `preview_execution { job_id }`
- **Pure read-only, process-free, provider-free**: Inspects `CoworkerWorkflowStore`, `ControllerPolicyStore`, and `AuthorityStore` without spawning processes, calling model providers, creating `sessions_dir` subdirectories, or acquiring physical job locks.
- **Concrete bounded scope**: Returns `approved: bool`, `readiness` (`needs_approval`, `missing_execution_envelope`, or `approved_and_frozen`), canonical `project_base` (`repository_path`, `base_commit`, `base_tree_sha`), `work_brief_id`/`work_brief_digest`, `plan_id`/`plan_revision`/`plan_digest`, `approval_id`/`approval_subject_digest`, `capability_envelope_digest`, `envelope_id`/`envelope_digest`, `max_parallel`, `approved_write_paths`, prompt-redacted `tasks`, `task_coder_choices`, `reviewer_choice`, `closure_choice`, `role_budgets`, correction/attempt caps, `allowed_check_ids`, path-redacted `check_commands` (basename `executable_name`, `argv`, `spec_digest`), and `pipeline_boundary`.
- **Admission readiness is distinct from durability inspection**: For a marked job, readiness requires a stored Complete receipt and matching immutable linkage/runtime. A fresh pure admission inspection can report `uncertain` because this process has no durability witness while `receipt.stage` is still `complete`; `preview_execution` may still report `approved_and_frozen`. Neither preview syncs or repairs persistence. Stored Uncertain/partial or conflicting admission cannot be made executable by a readiness read. Historical admission inspection remains available after supersession, without making that old job eligible for start.
- **Redaction guarantees**: Never leaks raw task prompt instructions, secret tokens, full host executable paths (`FrozenCheckCommandSpec::executable`), registry tool executable paths, or private logs.

### 2. `start_execution { job_id, expected_envelope_digest }`
- Verifies that `job_id` belongs to the enrolled `(binding_id, thread_uuid)`, is not superseded, has a verified `ApprovalRecord`, `FrozenApprovalLinkage`, `FrozenControllerCapabilityEnvelope`, and operator-attested `FrozenControllerExecutionEnvelope`, and that `expected_envelope_digest` matches the exact frozen `envelope_digest` in constant time **before** opening `CoworkerCodeDriver` or spawning any process.
- Unapproved jobs return `unauthorized_operator` (`needs_approval`); jobs missing capability or execution envelopes return `unauthorized_operator` or `not_configured` (`missing_execution_envelope`); mismatched digests return `validation_failed` (`envelope_digest_mismatch`) with zero process spawns and zero provider invocations. No alias, prefix, recomputation, or self-approval is accepted.
- **Complete-admission durability gate**: After exact digest and existing scope/planning/live-dependency checks, and before notice setup, driver open or owner spawn, start verifies complete admission. An unchanged stored `complete` receipt with `runtime_initialized: true` and independently matching linkage/marker/runtime may establish a fresh process-local witness by locking the job, syncing existing workflow/controller ledgers, receipt and plans directory, and rereading the subjects. Only successful sync returns count; no bytes/mtime, revisions, counters, leases, provider state or consent are changed by that step. Owner launch/ensure, code-driver reconcile and controller effect guards use the same gate. Pure status/preview/inspect and cancellation do not establish durability.
- Stored `uncertain`, `intent`, `linked` or `runtime_recorded`, missing mandatory receipts/runtime and conflicts stay fenced. Start never performs missing admission writes. Use explicit `flow coworker-admission reconcile --job-id ... --request-key ORIGINAL_KEY` after inspecting the original receipt. Reconcile retains original consent, source/resulting revisions, key and timestamp; it does not reset initialized runtime. Do not redispatch or generate a replacement key after `outcome_unknown` or `persistence_uncertain`.
- New-admission Worker, Reviewer and Closure consumers use the frozen canonical executable pins. Reviewer/Closure validate the pin choice against the envelope before passing its canonical path to the provider. A marked job cannot fall back to PATH; jobs without admission intent/marker retain legacy/manual resolution.
- When native notices are enabled, start also requires an actually verified enrolled origin and parent endpoint; this is transport configuration only and grants no execution or model authority.
- Resolves and validates the canonical same-package sibling `threadlane-flow` binary and invokes `CoworkerCodeDriver::ensure_owner`.
- **Single physical owner & idempotent duplicate calls**: Spawns a detached `threadlane-flow ... code-driver run-owner` process (returning once the owner's local Unix socket RPC is verified, without blocking MCP until the entire job finishes) or reuses the existing verified live owner (`disposition: "reused_live_owner"`) without duplicating provider calls.

### 3. `execution_status { job_id }`
- Verifies scoped job/approval/envelope admission and delegates to `CoworkerCodeDriver::status`.
- Reports `owner_state` (`live_verified_owner`, `no_active_owner`, `stale_crash_artifact_detected`, etc.), `job_phase`, legacy/diagnostic `overall_status`, `execution_mode`, `pipeline_status`, `pipeline_report`, `stopping_draining_in_flight`, `stop_requested`, `blocked_on_unknown_effect`, per-task stages, host observations, `native_notice` (including `latest`), and `native_notice_error` metadata.
- `overall_status` is not completion. `AllTasksIntegrated` is not completion; completion requires the required combined checks and exactly one accepted `Closure`. Older scopes may report only the diagnostic Phase 1 fallback.
- A status read after a notice/event is not a new dispatch; callers must not poll.

### 4. `stop_execution { job_id, reason? }`
- Verifies scoped job/approval/envelope admission and delegates to `CoworkerCodeDriver::cancel`.
- Persists durable `stop_requested = true` in `ControllerPolicyStore` **before** notifying any live owner over authenticated local Unix socket RPC or attempting narrow `ApprovedHostClient::cancel_task` cancellation (`durable_stop_recorded_before_host_cancel: true`).
- Signals original and derived Coder tasks, drains finite `Reviewer`/check/`Closure` calls, and fences late outputs. Remote inference termination is not proven. Never performs broad PID or process-name kills.

## Native notices (operator opt-in)

- `--coworker-native-notices` is operator-only and off by default on the raw CLI. Trusted managed setup enables it as transport configuration only.
- Native notice delivery requires an actually verified enrolled origin and parent endpoint before start. Missing notices or endpoint are not new execution authority or model permission.
- Execution terminal/held milestones and terminal planning-command notices use the same operator-only opt-in. The planning gateway configures its separate durable notice state before detached launch; its CLI completion hook asks Coworker to read `planning_command`. Installed desktop delivery remains a separate acceptance gate. Settled `BlockedByFailedChecks` and `AwaitingCorrectionExecution` attention notices are supported; running checks/corrections do not emit settled-block notices.
- `Submitted` means backend acceptance, not a read receipt or user acknowledgement.
- The private native-notices outbox is immutable: it reserves intent before send, does not retry an unknown possible-send, and bounds counts to 16. `execution_status` exposes `native_notice` / `latest` and `native_notice_error` metadata.
- Callers must not poll. When an event arrives, read `execution_status`; the read is not a new dispatch.

## Full automatic pipeline and cancellation boundaries

- **Full automatic pipeline**: When current frozen authorization permits, `CoworkerCodeDriver` runs automatic checks, `Reviewer` evaluation, bounded in-scope corrections, per-task `RunIntegration`, and final `Closure`. `AllTasksIntegrated` is not completion; completion requires the required combined checks and exactly one accepted `Closure`. `start_execution` requires the exact frozen `expected_envelope_digest`; no MCP path approves, freezes, or self-approves authority.
- **Diagnostic Phase 1 fallback**: Older scopes may expose only initial `Coder` dispatch and receipt reconciliation. Completed `Coder` runs report `ReadyForReview` / `AwaitingChecksOrReview` and are never accepted or integrated. Use `execution_mode`, `pipeline_status`, and `pipeline_report` to distinguish this fallback; `overall_status` is legacy/diagnostic and does not prove completion.
- **Stop and drain**: `stop_execution` records durable `stop_requested = true` first, signals original and derived Coder tasks, drains finite `Reviewer`/check/`Closure` calls, and fences late outputs (`durable_stop_recorded_before_host_cancel: true`). Remote inference termination is not proven. It never performs broad PID or process-name kills. `stopping_draining_in_flight` reports drain state.
- **Unknown post-launch outcomes**: Tasks or effects in `EffectUnknown` (`blocked_on_unknown_effect: true`) block automatic replay and require receipt reconciliation rather than blind retry.

## Evidence and acceptance limits

Regression sources exercise complete admission and cross-process detached-owner
execution with disposable fixtures and fake providers. They are not installed
package, live-provider, same-chat delivery or physical power-loss acceptance.
The parent owns combined formatting, behavioral tests and Clippy; this
documentation package does not report unfinished gates as passing. No live
admissions, jobs or providers were created. Larger runtime policy, decomposition,
user-language role outputs, startup ergonomics, native UI status and observation
performance remain follow-up work; generic dispatch/conversation-change races and
the missing-check-executable status/stop issue are not fixed here.
