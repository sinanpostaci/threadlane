# Profile Selection, Execution Policy, and Preflight Admission (Phase A1)

`crates/threadlane-native/src/profile_flow.rs` implements the **Phase A1** pure,
side-effect-free profile selection, role/difficulty resolution, and
deny-by-default execution policy admission core for Threadlane Milestone 4.

## Scope Boundary (Phase A1 vs Phase A2)

- **Implemented in Phase A1 (`profile_flow`)**:
  - Generic deny-by-default [`ExecutionPolicy`] (`schema_version = 1`,
    `policy_id`, and `allow_rules: Vec<PolicyAllowRule>`).
  - Structural policy validation ([`ExecutionPolicy::validate`]) and canonical
    64-character lowercase hex SHA-256 digest computation
    ([`ExecutionPolicy::digest`]).
  - Deterministic role/difficulty choice resolution ([`resolve_choice`]) that
    checks an exact `(role, Some(difficulty))` match first and falls back only
    to an explicit role default `(role, None)` without substituting another
    role, difficulty, or unapproved fallback model.
  - Pure, read-only preflight over a [`RegistrySnapshot`] ([`preflight`]) or an
    existing [`RegistryStore`] ([`preflight_from_store`]) using the store's
    lock-free readonly snapshot API.
- **Deferred to Phase A2 (not implemented yet)**:
  - Durable workflow cycle state, idempotent persisted role handoff records,
    and single-use package closure reservation.
  - Headless dispatch handoff through `ApprovedHostClient`.
  - The `threadlane-flow` binary and `threadlane-context ... flow ...` CLI
    routing.

## Core Invariants

1. **Explicit or real default profile selection only**:
   - When `PreflightRequest::profile_id` is `Some(id)`, preflight selects that
     exact profile ID.
   - When `PreflightRequest::profile_id` is `None`, preflight uses
     `RegistrySnapshot::default_profile_id`. It **never** infers a default from
     a profile whose `id` or `name` happens to be `"default"`. If
     `default_profile_id` is `None`, requested roles report
     `PreflightBlockReason::MissingDefaultProfile`.
2. **Requested roles only**:
   - Preflight resolves only the `(role, difficulty)` combinations listed in
     `PreflightRequest::roles` (rejecting empty, duplicate, or overlong lists).
   - Unrequested roles on the same profile (even if disabled, unassigned, or
     blocked by policy) never block the requested roles.
3. **Registry-sourced model and effort**:
   - `SelectedRoleChoice::model_arg` and `SelectedRoleChoice::effort` come
     exclusively from the resolved registry [`ModelDefinition`] and profile
     [`RoleAssignment`]. `RolePreflightRequest` accepts only `role` and optional
     `difficulty` (`#[serde(deny_unknown_fields)]`), preventing caller model,
     effort, tool, or executable overrides.
   - For Antigravity and OpenCode v2 executable worker routes, admission also
     runs the canonical pure `threadlane_workers::resolve_model_target` helper
     over the resolved registry model argument and effort. Antigravity therefore
     requires an explicit effort even when registry metadata sets
     `allow_no_effort`, and an OpenCode inline `#variant` becomes the effective
     effort used for policy evaluation. `SelectedRoleChoice::model_arg` remains
     the original registry model argument; `SelectedRoleChoice::effort` is the
     canonical effective effort. A missing or malformed executable worker target
     is blocked with `PreflightBlockReason::UnsupportedEffort` before policy
     evaluation and before any model invocation. Native Codex handoff does not
     resolve a worker model target and invokes no model during preflight.
4. **Truthful route classification**:
   - `ToolAdapter::CodexCli` maps to `RoleExecutionRoute::NativeHandoff` (never
     a headless worker).
   - `ToolAdapter::Antigravity` and `ToolAdapter::OpencodeV2` map to
     `RoleExecutionRoute::HeadlessWorker`.
   - `ToolAdapter::JevApi`, `ToolAdapter::OpenaiDecisionsApi`,
     `ToolAdapter::Git`, `ToolAdapter::Tmux`, and `ToolAdapter::Codegraph` map
     to `RoleExecutionRoute::Unsupported` and report
     `PreflightBlockReason::UnsupportedRoute`.
5. **Generic deny-by-default execution policy**:
   - Empty `allow_rules` is a valid policy that denies every role with
     `PreflightBlockReason::PolicyBlocked`.
   - Each [`PolicyAllowRule`] specifies an exact `(tool_id, model_id)` pair,
     allowed `efforts`, explicit `allow_no_effort`, and optional `roles`
     restrictions (`roles: []` applies to all roles).
   - No production model or provider names are hard-coded in code.
6. **Side-effect-free execution**:
   - `preflight` performs zero filesystem writes, creates no cycle/lock/store
     files, queries no providers, and starts no model, host, approval, or Git
     worktree.
   - `preflight_from_store` reads through `RegistryStore::snapshot_readonly`
     (the same parser is available statically as
     `RegistryStore::read_snapshot_readonly_at`), reusing the existing validated
     registry parser and private-directory checks without acquiring or creating
     `registry.lock`. A deleted lock stays absent and a missing directory stays
     absent. Atomic `registry.json` replacement yields a consistent complete
     snapshot.

## Public Rust API (`threadlane_native::profile_flow`)

- Constants:
  - `EXECUTION_POLICY_SCHEMA_VERSION` (`1`)
  - `MAX_POLICY_ALLOW_RULES` (`64`)
  - `MAX_PREFLIGHT_ROLES` (`30`)
- Types:
  - `PolicyAllowRule` (`tool_id`, `model_id`, `efforts`, `allow_no_effort`, `roles`)
  - `ExecutionPolicy` (`schema_version`, `policy_id`, `allow_rules`, `deny_all`, `validate`, `digest`, `allows`)
  - `RoleExecutionRoute` (`NativeHandoff`, `HeadlessWorker`, `Unsupported`)
  - `PreflightBlockReason` (`MissingDefaultProfile`, `MissingProfile`, `MissingAssignment`, `MissingModel`, `MissingTool`, `ToolDisabled`, `UnsupportedEffort`, `UnsupportedRoute`, `PolicyBlocked`)
  - `RolePreflightRequest` (`role`, `difficulty`)
  - `PreflightRequest` (`profile_id`, `roles`)
  - `SelectedRoleChoice` (`tool_id`, `adapter`, `route`, `model_id`, `model_arg`, `effort`, `matched_difficulty`)
  - `RolePreflightOutcome` (`role`, `requested_difficulty`, `ready`, `block_reason`, `detail`, `selected`)
  - `PreflightReport` (`registry_revision`, `profile_id`, `used_default_profile`, `policy_id`, `policy_digest`, `ready`, `roles`)
- Functions:
  - `resolve_choice(&RegistrySnapshot, profile_id, WorkflowRole, Option<Difficulty>) -> Result<ModelChoice, RegistryError>` (canonical resolver re-exported by `threadlane_workflow` and `threadlane_native`)
  - `preflight(&RegistrySnapshot, &PreflightRequest, &ExecutionPolicy) -> Result<PreflightReport, NativeError>`
  - `preflight_from_store(&RegistryStore, &PreflightRequest, &ExecutionPolicy) -> Result<PreflightReport, NativeError>`
- Registry readonly snapshot APIs:
  - `RegistryStore::snapshot_readonly()`
  - `RegistryStore::read_snapshot_readonly_at(config_dir)`
