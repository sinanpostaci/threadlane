# Role-flow operator CLI

Date: 2026-10-31. `threadlane-flow` is the packaged trusted-local operator CLI for
durable role-flow cycles. It is a thin command wrapper around the public
`threadlane_native::role_flow` APIs; it does not reimplement cycle state, policy
admission, handoff dispatch, reconciliation, or closure rules.

The binary is installed by the existing generic workspace Cargo/Nix package. This
document does not change packaging.

## Authority boundary

`--binding` and `--thread` must name the exact live Threadlane control binding and
native Codex thread UUID. Every operational command validates all five private
store directories and verifies that binding against the authenticated local
supervisor before it reads a request body or opens a store.

That check proves only that the target exists in the live local control plane. It
is not per-call native chat authentication, does not prove a human approved a
scope, and does not grant approval. Scope approval remains the separate explicit
`threadlane-approve` operation, and host startup remains the separate explicit
`threadlane-host serve` operation.

The CLI never selects a shell, executable, or child process from `PATH`. It starts
no host, model, provider, worker, approval, review, merge, or push.

## Fixed stores and flags

Every command requires the same fixed stores and target as `threadlane-session`:

```sh
threadlane-flow \
  --state-dir /absolute/private/runtime \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --sessions-dir /absolute/private/sessions \
  --binding BINDING_ID --thread THREAD_UUID \
  <command>
```

Global flags may appear before or after the subcommand. All directories must be
existing, canonical, private (`0700`), pairwise distinct, and non-overlapping.
`plans_dir` is the existing plan store root; durable role-flow state is owned by
`RoleFlowStore` under that root. No command creates a missing store directory.

Request files must be absolute, normalized, regular, private (`0600`), at most
1 MiB, and free of symlinks in every path component. Successful stdout is the
bounded untrusted-data operator envelope:

```json
{
  "untrusted_data": true,
  "route": "trusted_local_operator",
  "native_per_call_identity_verified": false,
  "binding_id": "bnd_...",
  "data": {}
}
```

## Commands

### `preflight`

```sh
threadlane-flow ... preflight \
  --request /absolute/private/preflight-request.json \
  --policy /absolute/private/execution-policy.json
```

Reads a `PreflightRequest` and `ExecutionPolicy`, then calls
`role_flow::preflight_from_dir_readonly(registry_dir, &request, &policy)`. The
canonical registry snapshot is read without opening `open_workflow_stores`, an
`AuthorityStore`, or a role-flow store. Preflight creates no `registry.lock`,
plan, authority, session, or role-flow file and performs no Git or provider call.

`data` is the Phase A1 `PreflightReport`: registry revision, selected profile,
policy digest, overall readiness, and per-role block reasons or selected
model/effort/route. A blocked role is a successful report with `ready: false`;
the CLI does not turn a block into an approval or a dispatch.

### `create`

```sh
threadlane-flow ... create --request /absolute/private/create-request.json
```

Reads a `CreateRoleFlowCycleRequest`. Its `binding_id` and `thread_uuid` must
exactly equal the fixed CLI target before any authority or role-flow store is
opened. A foreign body is rejected with `scope_mismatch`; the CLI never rewrites
the body to the CLI scope. A valid body opens the existing workflow and authority
stores, then calls `RoleFlowStore::create_cycle(&request, &authority)` and emits
the returned `RoleFlowCycle`.

The backend enforces approval existence, frozen task selection, ledger revision,
profile, policy, and idempotency rules. Creating a cycle does not start a host or
a worker.

### `get`

```sh
threadlane-flow ... get --cycle-id flw_0123456789abcdef0123456789abcdef
```

Opens only the read-only role-flow store and calls
`get_cycle(binding_id, cycle_id)`. A missing cycle is `cycle_not_found`; a found
cycle must also have the fixed `thread_uuid` or the command reports a scope
failure. Full task instructions are returned only for this explicit single-cycle
read, not by `list`.

### `list`

```sh
threadlane-flow ... list [--limit 20] [--cursor flw_...]
```

Opens only the read-only role-flow store and reads the binding snapshot. It emits
a bounded page of newest-first summary rows with cycle ID, cycle revision,
approval/plan/profile IDs, registry revision, policy ID/digest, task count,
handoff count, and closure-reservation count. Task instructions, titles, prompts,
and handoff evidence payloads are not included.

`--limit` must be `1..=20` and defaults to `20`. `--cursor` is the last returned
`cycle_id`; it must be a bounded identifier and must exist in the binding's
current cycle list. The page reports `ledger_revision` and `next_cursor` instead
of serializing every full cycle.

### `handoff`

```sh
threadlane-flow ... handoff \
  --request /absolute/private/handoff-request.json \
  [--confirm-approval SHA256] [--operator-approved]
```

Reads a `RoleHandoffRequest` and rejects a body whose binding/thread differs from
the fixed CLI target. The JSON `operator_approved` and
`confirm_approval_digest` fields are always replaced from the actual CLI flags:
JSON can never self-grant approval. `--confirm-approval` must be a lowercase
64-character SHA-256 digest. The request's optional `closure_reservation_id` is
read generically and preserved for the backend.

The command then opens the existing authority store and an `ApprovedHostClient`
scoped to the fixed binding/thread and calls
`RoleFlowStore::handoff_role(&request, &authority, Some(&client))`. The backend
enforces the frozen task, current policy, operator approval, closure reservation,
and native-vs-headless route. A native handoff is a recorded manual routing
request, not proof that a model executed it. A headless handoff may dispatch only
through an already-running approved host; this CLI does not start that host.

### `reconcile`

```sh
threadlane-flow ... reconcile --request /absolute/private/reconcile-request.json
```

Rejects foreign scope, opens the existing authority store and scoped
`ApprovedHostClient`, then calls
`RoleFlowStore::reconcile_handoff(&request, &client).await`. Reconciliation reads
the actual approved task receipt. It never dispatches, cancels, restarts, or
replays a task. Uncertain prior outcomes remain uncertain until the actual
receipt is observed.

### `reserve-closure`

```sh
threadlane-flow ... reserve-closure \
  --request /absolute/private/reserve-closure-request.json
```

Rejects foreign scope, opens only the role-flow store, and calls
`RoleFlowStore::reserve_closure(&request)`. The backend accepts only the actual
approved cycle base candidate (`ClosureSubjectSource::ApprovedCycleBase`) and
enforces single-use reservation per repository/package/candidate tree. This
command makes no provider or host call and does not accept an unverified
descendant subject.

## Error and outcome handling

Errors use a static, bounded JSON envelope on stderr:

```json
{"code":"scope_mismatch","message":"..."}
```

Native role-flow errors preserve their stable code and a sanitized static
message; raw CLI/provider diagnostics, prompts, request bodies, and filesystem
paths are not echoed. Timeout, I/O, or ambiguous transport failures after a
mutating call are reported as `outcome_unknown`. Do not blindly retry them:
inspect the cycle, handoff, task receipt, or closure ledger first.

Failures do not write partial result JSON to stdout. A failed command does not
prove that no effect occurred when the code is `outcome_unknown`.

## Current limitations

- This CLI is a wrapper; it requires the role-flow store API and backend behavior
  in `threadlane_native::role_flow`. Missing backend behavior is an integration
  failure, not a CLI success.
- Preflight is an admission report, not an approval, reservation, or dispatch.
- `handoff` records manual native routing or requests backend dispatch only
  through an already-running approved host. It never proves native model output.
- `reconcile` and `reserve-closure` are explicit operator actions. Neither
  automatically approves, accepts, merges, pushes, or starts a host.
- `list` intentionally omits prompts and instructions. Use `get` for one explicit
  cycle inspection.
- There is no transcript monitoring, automatic approval detection, authenticated
  two-chat routing, or implicit same-project child dispatch in this wrapper.
- Linux is the tested platform. Non-Unix platforms fail closed as unsupported.
