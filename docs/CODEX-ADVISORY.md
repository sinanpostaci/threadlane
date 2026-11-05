# Read-Only Codex CLI Advisory Workers (`CODEX-ADVISORY`)

This document specifies the support matrix, launch and security boundaries, evidence provenance, and configuration requirements for read-only headless `ToolAdapter::CodexCli` (`WorkerProvider::CodexCli`) advisory worker execution.

## 1. Role and Route Support Matrix

| Workflow Role | Conversational vs. Headless | `ToolAdapter::CodexCli` Support | Route & Normalization Boundary |
| --- | --- | --- | --- |
| `Follower` (Native Coworker) | Human-selected native Codex conversation only (`COWORKER_IS_CONVERSATIONAL_INTERFACE_ONLY = true`) | **Never headless** (no model profile slot or lock; never dispatched as a worker) | Always rejected by `is_read_only_advisory_worker_role`, `normalize_read_only_advisory_choice`, and `validate_non_native_worker_choice`. |
| `Orchestrator` | Read-only planning worker (`CoworkerPlanningService`) | **Supported (read-only advisory)** | Generic `RoleExecutionRoute::from_adapter(CodexCli)` is `NativeHandoff`; explicit `normalize_read_only_advisory_choice` after exact `RegistrySnapshot` + `ExecutionPolicy` preflight normalizes to `HeadlessWorker`. |
| `Architect` | Read-only architectural advice worker (`CoworkerPlanningService`) | **Supported (read-only advisory)** | Same explicit post-preflight normalization to `HeadlessWorker` under an active `PlanningAdviceGrant`. |
| `Reviewer` | Read-only task candidate review worker | **Supported (read-only advisory)** | Same explicit post-preflight normalization to `HeadlessWorker` when authorized by `ExecutionPolicy` and `FrozenApprovalLinkage::authorizes_reviewer_dispatch` (via the frozen `FrozenControllerCapabilityEnvelope`). |
| `Closure` | Read-only combined-candidate package closure worker | **Supported (read-only advisory)** | Same explicit post-preflight normalization to `HeadlessWorker` when authorized by `ExecutionPolicy`, `FrozenApprovalLinkage::authorizes_closure_dispatch` (via the frozen `FrozenControllerCapabilityEnvelope`), and a single-closure `PackageClosureReservation`. |
| `Coder` | Mutating task implementation worker | **Unsupported for headless `CodexCli`** | Rejected by `is_read_only_advisory_worker_role`, `normalize_read_only_advisory_choice`, `validate_non_native_worker_choice`, and `WorkerSpec`. |

- **Generic and manual paths unchanged**: `RoleExecutionRoute::from_adapter(ToolAdapter::CodexCli)` and manual role-flow handoffs (`RoleFlowStore::create_cycle` / `handoff_role`) remain `RoleExecutionRoute::NativeHandoff` and never dispatch a headless worker.

## 2. Configuration and Authority Gates

Saved profiles and registry catalogs never grant execution authority on their own. Running a read-only `CodexCli` advisory worker requires:

1. **Enabled registry tool**: A `ToolDefinition` with `adapter: ToolAdapter::CodexCli` and `enabled: true` in the frozen `RegistrySnapshot`. `resolve_advisory_tool_executable` resolves symlinks and validates the canonical executable from a configured path or resolves `"codex"` (or a bare binary name) from `PATH`.
2. **Exact model and explicit reasoning effort**: A `ModelDefinition` (`model_arg`) and a `RoleAssignment` with an explicit `effort: Some(...)` supported by the model. `resolve_model_target(WorkerProvider::CodexCli, ...)` and `normalize_read_only_advisory_choice` reject `effort: None`.
3. **Explicit `ExecutionPolicy` allow rule**: A `PolicyAllowRule` matching `(tool_id, model_id, effort)` and permitting the target read-only role.
4. **Unchanged workflow authority envelopes**:
   - `Orchestrator` and `Architect` require an active, operator-attested, non-expired `PlanningAdviceGrant` with remaining call budget (`docs/COWORKER-PLANNING.md`).
   - `Reviewer` and `Closure` require a bound `FrozenApprovalLinkage` with an explicit `FrozenControllerCapabilityEnvelope` (`FrozenApprovalLinkage::authorizes_reviewer_dispatch` / `FrozenApprovalLinkage::authorizes_closure_dispatch`) and, for `Closure`, a matching `PackageClosureReservation` (`docs/COWORKER-WORKFLOW-CONTRACT.md`, `docs/PROFILE-FLOW.md`).

## 3. Launch and Security Boundaries

Read-only `CodexCli` invocations use `AdvisoryWorkerSpec` (`provider: WorkerProvider::CodexCli`, `file_permissions: WorkerFilePermissions::ReadOnly`, empty `allowed_write_paths: []`, `standalone: false`, `session_id: None`) and enforce the exact pinned argument vector (`is_pinned_codex_read_only_argv`):

```text
codex --no-daemon exec --json --ephemeral --ignore-user-config \
  --model <cli_model_arg> \
  -c model_reasoning_effort="<effort>" \
  -c approval_policy="never" \
  -c model_provider="openai" \
  -c default_permissions="threadlane-advisory" \
  -c 'permissions.threadlane-advisory={"filesystem"={":minimal"="read",":workspace_roots"={"."="read","**/.env"="deny","**/.env.*"="deny","**/codex.toml"="deny","**/.codex.toml"="deny","**/.git"="deny","**/.git/**"="deny","**/.codex"="deny","**/.codex/**"="deny","**/.agents"="deny","**/.agents/**"="deny","**/.aws"="deny","**/.aws/**"="deny"},"/nix/store"="read"},"network"={"enabled"=false}}' \
  -c web_search="disabled" \
  --disable hooks --disable plugins --disable apps --disable multi_agent \
  --disable browser_use --disable computer_use --disable in_app_browser \
  --disable image_generation --disable memories --disable skill_search \
  --disable skill_mcp_dependency_install --disable shell_snapshot \
  -- <prompt>
```

- **Zero write scopes and clean workdir checks**: `allowed_write_paths` must be empty. `validate_codex_advisory_workdir` requires a canonical non-root directory and fails closed if any project-local Codex configuration or hook marker (`CODEX_PROJECT_CONFIG_MARKERS`: `.codex`, `codex.toml`, `.codex.toml`) exists in or above the working directory. Source and snapshot checkouts are verified clean before and after execution.
- **No second app-server or persistent daemon**: Each invocation runs as an ephemeral (`--no-daemon`, `--ephemeral`, `--ignore-user-config`) child in its own supervised Unix process group (`process_group(0)`). Threadlane never starts a second shared Codex app-server or installs a persistent OS service.

## 4. Stream Framing, Evidence Provenance, and Usage

- **Bounded final JSON capture**: `run_advisory_worker_with_structured_result` captures only a completed `type: "item.completed"` event with `item.type == "agent_message"` and a non-empty UTF-8 top-level JSON object `item.text` inside a verified `thread.started` -> `turn.started` -> `turn.completed` lifecycle (`AdvisoryResponseSource::CodexAgentMessageTurnCompleted`). Terminal `turn.failed` or `error` events, missing `thread.started` / `turn.completed` framing, conflicting `thread_id` values, duplicate final agent messages, or tool/command spoofing fail closed. Timeout, cancellation, `EffectUnknown` no-auto-replay, and idempotent duplicate recovery invariants are preserved.
- **Requested vs. observed model/effort (no fallback)**: Actual Linux `codex exec --json` JSONL output does not emit runtime model or reasoning-effort telemetry. `requested_model` and `requested_effort` come from the verified exact pinned launch configuration (`--model` and `-c model_reasoning_effort`) and **must never** be presented as or copied into observed telemetry (`observed_model` and `observed_effort` remain `None` unless genuinely emitted by the CLI).
- **Review and closure evidence provenance**: Pinned Codex launch configuration evidence is explicit and separate from runtime-observed stream telemetry; absent observed model/effort remains `None` with no `requested -> observed` fallback, while legacy `Antigravity` and `OpencodeV2` runtime model/effort telemetry gates remain strict.
- **Genuine token usage (`codex_cli` only)**: When `turn.completed` includes genuine token counts (`input_tokens`, `output_tokens`, optional `cached_input_tokens`), they are captured in `StreamTokenUsage` and projected into `ObservationUsage` (`provenance: "worker_stream"`). Dollar costs, estimates, and synthetic zero usage are never fabricated, and `Antigravity` / `OpencodeV2` usage remains `None`.
- **Independent Activity requester metadata**: Paired MCP requester `selected_model` and `selected_effort` on `ObservationActor` come from bounded once-per-call metadata-only `thread/read` (`includeTurns: false`) over the existing verified managed parent WebSocket endpoint (clearing to `None` on any refresh error or mismatch, with `actor.model` / `actor.effort` remaining `None` and `native_per_call_identity_verified == false`), independent of headless worker execution telemetry. See [`docs/COWORKER-MODEL-OBSERVATION.md`](COWORKER-MODEL-OBSERVATION.md) and [`docs/COWORKER-OBSERVATION.md`](COWORKER-OBSERVATION.md).

## 5. Verification and Platform Qualifications

- **Linux-tested / non-Linux qualifications**: Process-group supervision, `0700`/`0600` private filesystem gates, and `codex_cli` advisory validation are tested on Linux, and no macOS or Windows acceptance is claimed; the managed native pairing route is Linux-gated and other platform paths must be separately validated.
- **Protocol framing vs. live runtime acceptance**: A source-free protocol probe and local fake-CLI fixture tests verified `codex exec --json` CLI framing and supervision invariants only; they do not establish installed desktop runtime or live end-to-end paid-provider acceptance.
