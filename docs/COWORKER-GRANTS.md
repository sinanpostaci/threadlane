# Coworker planning and advice grant CLI

`threadlane-flow` exposes five trusted-local operator subcommands for standing
read-only Coworker planning and architectural advice grants
([`threadlane_native::coworker_grants`](../crates/threadlane-native/src/coworker_grants.rs)):
`grant-prepare`, `grant-record`, `grant-revoke`, `grant-list`, and `grant-get`.

## Authority and provenance boundary

Every command requires the existing fixed global store and target flags:

```sh
threadlane-flow \
  --state-dir /absolute/private/runtime \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --sessions-dir /absolute/private/sessions \
  --binding BINDING_ID --thread THREAD_UUID \
  <subcommand>
```

- `validate_dirs` requires all five directories to be existing, canonical,
  private (`0700`), pairwise distinct, and non-overlapping.
- `verify_binding` authenticates the live local supervisor binding at
  `state_dir` and confirms that `BINDING_ID` owns the canonical non-nil
  `THREAD_UUID`. This verifies local runtime binding state only; it does **not**
  cryptographically prove human conversation origin or per-call native identity
  (`route: "trusted_local_operator"`, `native_per_call_identity_verified: false`).
- Saved workflow profiles, model/worker prose, and MCP tool calls never grant
  authority. No MCP self-approval or grant-creation method exists.
- Recording a planning grant never starts a provider, host, or worker job and
  never grants code-execution write approval
  (`capabilities.code_execution_approval_granted: false`).

## Subcommands and JSON types

Request files must be normalized, absolute, non-symlink regular files with
private `0600` permissions (`<= 1 MiB`). Successful stdout uses the bounded
(`<= 1 MiB`) operator envelope; errors emit `{"code":"...","message":"..."}` on
stderr without echoing request bodies, prompts, or secrets.

### 1. `grant-prepare`

```sh
threadlane-flow ... grant-prepare \
  --request /absolute/private/prepare-grant-request.json
```

- **Input**: `PreparePlanningGrantRequest` (`request_key`, `binding_id`,
  `thread_uuid`, `repository_path`, `base_commit`, optional
  `expected_base_tree_sha`, explicit `profile_id`, optional
  `expected_registry_revision`, `roles` for `Orchestrator`/`Architect`,
  deny-by-default `policy`, strict read-only `capabilities`, finite `budget`,
  `created_at_ms`, and `expires_at_ms`).
- **Behavior**: Verifies `binding_id` and `thread_uuid` against the fixed CLI
  scope before any store or helper call, then runs
  `prepare_planning_grant_preview_from_dir_readonly` and
  `prepare_planning_grant_preview_with_store`. Bounded read-only Git
  verification of the canonical repository and base commit/tree is allowed.
- **No mutation side effects**: Creates no store files, lockfiles
  (`registry.lock` or `coworker-workflow-*.lock`), call reservations, or
  `observation-journal.jsonl` entries, even when `THREADLANE_PARENT_REQUEST_ID`
  is set. Never opens `AuthorityStore` or `RoleFlowStore`.
- **Output (`data`)**: `PlanningGrantPreview` with
  `status: "proposed_candidate_not_granted"`, `grant_active: false`,
  `frozen_role_choices`, `policy_digest`, `capabilities`, `budget`,
  `created_at_ms`, `expires_at_ms`, and canonical 64-hex `preview_digest`.

### 2. `grant-record`

```sh
threadlane-flow ... grant-record \
  --request /absolute/private/record-grant-request.json \
  --confirm-preview PREVIEW_SHA256 \
  --operator-approved
```

- **Input**: `RecordPlanningGrantRequest` JSON body (`request_key`, optional
  `expected_ledger_revision`, `preview`, plus `confirmed_preview_digest` and
  `attestation` replaced/enforced from CLI flags) alongside explicit CLI
  `--confirm-preview <64hex>` and `--operator-approved`.
- **Confirmation rules**: A JSON file with `operator_attested: true` or
  `confirmed_preview_digest` is never honored by itself. Missing
  `--operator-approved` fails with `unauthorized_operator`; missing or malformed
  `--confirm-preview` fails with `invalid_input`; a CLI/preview digest mismatch,
  tampered preview field, or stale live registry revision fails with
  `validation_failed` without silent profile or model substitution. Attestation
  always enforces `native_per_call_identity_verified: false` and
  `derived_from_model_or_mcp_prose: false`.
- **Output (`data`)**: Persisted metadata-only `PlanningGrantSummary` via
  `record_planning_grant_from_registry_dir`. Records mutation entry/finish in
  the observation journal.

### 3. `grant-revoke`

```sh
threadlane-flow ... grant-revoke \
  --request /absolute/private/revoke-grant-request.json
```

- **Input**: `RevokePlanningGrantRequest` (`binding_id`, `thread_uuid`,
  `grant_id`, required `expected_ledger_revision`, `revoked_at_ms`, and bounded
  `reason`).
- **Behavior**: Enforces fixed CLI `TrustedGrantScope` and compare-and-swap
  `expected_ledger_revision` via `revoke_planning_grant_scoped`. Foreign
  binding/thread metadata is rejected before touching any foreign ledger.
- **Output (`data`)**: Updated `PlanningGrantSummary` (`status: "revoked"`).

### 4. `grant-list` and `grant-get`

```sh
threadlane-flow ... grant-list
threadlane-flow ... grant-get --grant-id pgr_0123456789abcdef0123456789abcdef
```

- **Behavior**: Pure read-only queries via `list_planning_grant_summaries` and
  `get_planning_grant_summary` under `TrustedGrantScope`. Neither command
  creates store files, lockfiles, or observation journal records (even when
  `THREADLANE_PARENT_REQUEST_ID` is set), and neither acknowledges events or
  dispatches work. Missing grants return `grant_not_found`.
- **Output (`data`)**: `Vec<PlanningGrantSummary>` (`grant-list`) or
  `PlanningGrantSummary` (`grant-get`), containing only bounded grant metadata,
  digests, budgets, usage counters, status, and reservation counts.

## Scope of this checkpoint

This checkpoint wires the trusted-local operator CLI and behavioral tests for
standing planning/advice grant preparation, recording, revocation, and
metadata-only inspection. Async read-only Orchestrator/Architect execution over
grant reservations (`planning.rs`), automatic routine task/review/fix scheduling
(`controller.rs`), and combined candidate assembly (`candidate.rs`) remain
separate pending integration stages.
