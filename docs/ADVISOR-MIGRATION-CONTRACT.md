# Advisor role migration contract

The current optional architectural consultation role is named **Advisor**. New
workflow role identities use the `advisor` key. Existing frozen `architect`
records remain readable, valid, and byte/digest stable. This supplements the
minimal chat MVP; advice remains optional and grants no Worker authority.

## Role identity and compatibility

1. Preserve raw legacy identity. A deserialized legacy `architect` role must
   serialize back to `architect`. Do not globally deserialize both spellings to
   one variant and then emit a different spelling: that invalidates old digests.
2. Represent a new `advisor` identity distinctly from the legacy identity, for
   example `WorkflowRole::Advisor` and the retained legacy `Architect` variant.
   `Advisor.as_str()`/serde emit `advisor`; legacy Architect emits `architect`.
   Both display as Advisor. The five current execution roles contain Advisor
   once; a legacy variant does not add a sixth configurable role.
3. Provide a small explicit semantic comparison/normalization boundary, such as
   `canonical()` (Architect becomes Advisor) and `is_advisor()`. Use it for
   role lookup, default/override selection, duplicate detection and advisory
   eligibility. Preserve raw identity for frozen serialization and hashing.
4. Reject simultaneous legacy/new assignments for the same semantic role and
   difficulty. Do not merge their models, choose the first/last, silently
   substitute a model, or broaden a role policy.
5. Existing frozen plans, grants, policies, approvals, artifacts and receipts
   retain their original fields, raw role spelling, revision, IDs and hashes.
   Reads and observation never migrate a store or save updated profiles.
6. Standing-grant profile dependency hashes must remain stable for the equivalent
   old/new profile spelling. Preserve the existing v1 canonical hash domain
   explicitly when constructing/comparing ScopedProfileDependencySpec. Its legacy
   canonical hash representation may retain architect; explain this internal
   compatibility domain rather than rewriting old proof records. Keep actual
   new workflow role identities as advisor outside that digest domain.

## New and retained advisory entrypoints

- New published tasks, current profile role choices, new planning grants, role
  cycles/policies, new advisory requests/artifacts and current observation use
  Advisor where newly created. Existing authorities continue in their frozen
  identity; never retarget a running/frozen old scope merely through an alias.
- Add a canonical `consult_advisor` MCP entrypoint (and the corresponding local
  flow command/service path where that interface exists). Preserve
  `consult_architect` and old command/request variants for existing callers and
  immutable command replay. Reuse the same execution, permissions and budgets.
- Preserve provenance for persisted command/request kinds as needed: old command
  records must not change payload digests or start again after a restart. New
  Advisor requests under new Advisor authority write Advisor role identities.
- If continuing old authority through a new entrypoint is not identity-safe,
  explicitly require the legacy entrypoint. Do not silently change frozen role,
  request identity, model, executable, call count or digest to make it work.
- Legacy compatibility field/outcome names such as max_architect_rounds,
  accepted_architect_decisions and RequiresArchitectAdvice may remain readable
  API identifiers. They are not permission to keep every newly created role as
  architect. Do not rename unrelated DecisionAdvisor/Jev/OpenAI Decisions data.
- Update the new-call title to Advisor consultation. Stored historical event
  text, action, timestamp and outcome are preserved. Optional advice never becomes
  a mandatory stage in ordinary per-chat backlog execution.

## Frontend boundary

Canonical current role arrays, profile defaults/overrides, new draft values and
new configuration writes use advisor. Legacy architect payloads still load and
render without losing choices or creating a second role row. Read-only historical
records retain raw identity. Normalize before semantic lookup and duplicate
checking; do not silently rewrite frozen records when rendering them.

Keep the approved live Coworker/Worker header, compact footer, stream, icons,
scroll behavior, timers and four backlog states unchanged. Both role labels are
Advisor. Software text and source are English; agent-authored reports follow the
human's language.

## Required acceptance

- Golden pre-transition global Architect/RoleAssignment/ScopedProfileDependencySpec
  bytes and digest remain exactly unchanged. Add new Advisor serialization tests.
- Legacy approval/grant/policy/plan/artifact/command fixtures load and validate
  without file, revision, digest or cursor mutation; original command replay
  produces no new model invocation.
- New Advisor profile/default/override and new plan/grant/advice paths persist
  advisor. Verify the actual role field in the new stored artifact, not only its
  visible label or a permissive deserializer.
- Test the canonical advisory MCP/CLI path with deterministic fake providers,
  and retain legacy path coverage. No paid advisory call is authorized by this
  migration test requirement.
- Test dual-alias duplicates, missing choice, stale revisions, role-policy/model
  mismatch, cross-chat isolation, replay and reopening. No test may be weakened
  to accommodate a faulty migration or unapproved dispatch.
- Run the combined Rust behavior suites, formatting/Clippy, frontend checks,
  browser suite, final packages and isolated native/VM acceptance. State actual
  platform and provider boundaries accurately.

## Parallel ownership

The Rust/backend owner changes Rust and the backend compatibility documentation.
The frontend owner changes only its frozen TypeScript/UI files. Both work from
the same exact base in independent task checkouts; the parent alone integrates
and validates. No writer changes the other owner's files. The primary agent
does not mark the goal complete after this foundation or a passing subset.
