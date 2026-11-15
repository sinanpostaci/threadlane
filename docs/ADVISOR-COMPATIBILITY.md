# Advisor role compatibility

The current optional architectural consultation role is **Advisor** (`advisor`).
The retained legacy identity is **Architect** (`architect`). They are the same
semantic advisory role, but they are deliberately distinct stored values so raw
legacy bytes and digests never change merely because the product name changed.

## Current identity versus frozen legacy identity

- `WorkflowRole::Advisor` serializes and parses as `advisor`; its display label is
  `Advisor`.
- `WorkflowRole::Architect` serializes and parses as `architect`; it also displays
  as `Advisor` because the product role is the same. It exists only for frozen v1
  plans, approvals, policies, receipts, command replay, and historical records.
- A deserialized legacy `architect` value serializes back to `architect`. Reads
  never migrate a store, rewrite a frozen record, or mutate a digest.
- `WorkflowRole::canonical()` maps legacy `architect` to current `advisor`;
  `is_advisor()` recognizes both spellings. These predicates are used only at
  explicit semantic lookup, eligibility, default/override selection, and duplicate
  detection boundaries.
- The five configurable execution roles are Orchestrator, Advisor, Worker
  (canonical `coder`), Reviewer, and Closure. Retained legacy `Architect` is the
  same advisory slot and does not add a sixth configurable role.

## Mutable configuration and new work use advisor

- New profile assignments and configuration writes use `role: "advisor"`. The
  `WorkflowProfile` mutable configuration projection writes both advisory spellings
  as `advisor`; legacy `architect` profile payloads still load without losing a
  choice or creating a second role row.
- New plan tasks submitted as `advisor` store `advisor` in the published plan
  revision and compiled task. The retained `architect` input still stores
  `architect` for exact historical replay. Role resolution is semantic in either
  direction, so an advisory task resolves the same exact model and effort from
  either advisory assignment spelling.
- New planning-grant policies, reservations, command records, private planning
  artifacts, and current observation records store the actual `advisor` identity
  when they are created through the canonical path.
- Simultaneous `advisor` and `architect` assignments for the same semantic role
  and difficulty are rejected before choice lookup. They are never merged,
  last-write-wins, or silently substituted.
- The optional `decision_advisor` (`DecisionAdvisor` / Jev / OpenAI Decisions)
  setting is a separate shadow-advisor configuration and is unchanged.

## Canonical and retained advisory entrypoints

- `consult_advisor` is the canonical MCP entrypoint. It enqueues a durable
  `kind: "advisor"` planning command, writes an Advisor reservation and artifact,
  and titles the new observation `Advisor consultation`.
- `consult_architect` and existing `kind: "architect"` commands remain available
  for existing callers and exact command replay. They keep the frozen legacy
  Architect identity, payload digest domain, and no-redispatch behavior.
- `consult_advisor` requires planning authority whose policy explicitly names
  `advisor`. A pre-transition grant that only names `architect` is not silently
  retargeted; the caller must use the retained `consult_architect` entrypoint.
- Both entrypoints reuse the existing execution, read-only sandbox, permission,
  budget, and artifact paths. Advice remains optional and never becomes a
  mandatory stage in ordinary per-chat backlog execution.
- Legacy field, outcome, and tool names such as `consult_architect`,
  `accepted_architect_decisions`, `RequiresArchitectAdvice`,
  `requires_architect_advice`, `max_architect_rounds`, and
  `max_architect_calls` remain readable compatibility identifiers. They do not
  force newly created role identities to remain `architect`.

## Frozen digests and the v1 compatibility domain

The following surfaces keep their canonical `architect` spelling, ordering, and
hash inputs:

- published or frozen plan tasks that already contain `architect`;
- approval records and subject digests;
- receipts and historical plans;
- global `WorkflowRole` / `RoleAssignment` serialization for a legacy value;
- retained legacy command payloads and request-body digests.

Two internal compatibility projections preserve old hashes without rewriting
anything:

1. `ScopedProfileDependencySpec` projects current `advisor` assignments to the
   stable v1 spelling `architect` when it constructs or validates the dependency
   proof. Equivalent old/new profile spellings therefore produce identical
   dependency bytes and standing-grant dependency digests. The registry profile
   itself keeps its raw `advisor` value; only this internal proof representation
   uses the legacy spelling.
2. `ExecutionPolicy` digests project current `advisor` roles to `architect` and
   expand an empty role list over the stable v1 six-role hash set. A new policy
   that names `advisor` therefore has the same digest as the equivalent
   pre-transition policy that named `architect`, while semantic policy matching
   still authorizes the actual current role at runtime.

These are digest-domain projections only. New grants, plans, requests,
reservations, artifacts, and observation records retain `advisor` outside them.

## Storage and downgrade limits

- Registry, plan, workflow, and planning-command schema versions remain `1`; no
  store migration is required.
- Reading a legacy registry snapshot does not rewrite `registry.json`, change its
  revision, or alter its bytes. Advisor keys are written only on an actual
  explicit registry mutation.
- A mutation that saves a profile writes `advisor` into mutable profile
  configuration. Older binaries from before this transition do not understand
  that key; downgrading therefore requires restoring an appropriate registry
  backup. Threadlane does not silently rewrite the store for an older binary.
- An already-frozen approval, plan, policy, receipt, grant, command, or running
  role-flow cycle is not rewritten or retargeted by this change.
- This source compatibility does not by itself prove live Advisor execution, a
  provider call, or an installed-host upgrade. Those remain separate acceptance
  gates.
