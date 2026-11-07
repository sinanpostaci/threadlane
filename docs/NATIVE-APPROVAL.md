# Native conversation scope approval

Date: 2026-10-31. This document describes the additive local operator route for
freezing a concrete, already-presented execution scope from a native Codex
conversation. The desktop approval path and the persisted approval schema remain
unchanged.

## What the route is

`threadlane-approve` is a separate, operator-only CLI in the `threadlane-native`
package. It has two commands:

- `prepare` reads an `OperatorApprovalIntent`, validates it against the current
  published plan, registry, repository, scopes and live configured binding, and
  returns an `OperatorApprovalPreview` without writing an approval.
- `record` reads the exact `OperatorApprovalRequest` from that preview, requires
  three explicit confirmations, and persists an `ApprovalSummary` through the
  existing authority ledger.

The preview includes the full frozen task model/effort/CLI pins, executable
paths, write scopes, instructions and budgets. It is local operator output, not
a persisted approval. The only persisted artifact from this route is the existing
`ApprovalRecord` format with its existing subject digest and policy version.
Neither command starts a worker or provider, creates a worker session or
worktree, or performs review acceptance, merge or push.

## Authority boundary

Native Codex interprets the human's explicit approval of a concrete scope. This
CLI is the trusted local operator's mechanism for attesting that decision. It is
not a transcript listener, a classifier, a yes-text heuristic, or an automatic
approval detector. MCP tools, workers and ordinary model prose cannot grant
authority, and the native MCP surface exposes no self-approval tool.

The confirmation flags are explicit operator attestations. They are **not**
authentication, cryptographic proof of a human conversation, or proof that a
particular native chat produced the request. The live-binding check confirms only
that the configured supervisor still has the exact `binding_id` and canonical
`thread_uuid`; it does not authenticate the human or establish general multi-chat
provenance. See [native routing](NATIVE-ROUTING.md) for the unresolved two-chat
and per-call identity gates.

Recording an approval does not dispatch a task, start a worker or provider, create
a worktree, accept review work, merge, or push. Starting an approved task remains
a separate explicit action.

## Commands

All paths must be absolute and normalized. The four store directories must be
canonical, private, pairwise distinct and non-overlapping. Store flags
(`--state-dir`, `--registry-dir`, `--plans-dir`, `--authority-dir`) and optional
paired fixed-scope flags (`--binding`, `--thread`, and optional `--project-root`)
are accepted either before or after the subcommand. When routed through
`threadlane-context --context <ABS_PRIVATE_JSON> approve <prepare|record> ...`,
the context router always supplies the four bound stores plus `--binding`,
`--thread`, and `--project-root` from the verified context, and `threadlane-approve`
enforces those fixed values against the decoded `OperatorApprovalIntent` or
`OperatorApprovalRequest` (`binding_id`, `thread_uuid`, `--confirm-thread`, and
`repository_path`) before any ledger effect so request-file replacement cannot
route a different chat or repository through a checked context.

```sh
# Via context router (recommended when operator_context is available):
threadlane-context --context /absolute/private/context.json \
  approve prepare --request /absolute/private/intent.json

# Direct unscoped legacy invocation:
threadlane-approve prepare \
  --state-dir /absolute/private/runtime \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --request /absolute/private/intent.json
```

`prepare` accepts an `OperatorApprovalIntent`. It has the same fields as
`OperatorApprovalRequest` except that it omits `expected_plan_digest`; the
authority compiles the exact active plan revision and fills that digest without
upgrading the requested plan or registry revisions. Successful stdout is the
`OperatorApprovalPreview` JSON document, including `request`, `subject_digest`
and the frozen `tasks`.

Save `preview.request` as a private request file. Do not hand-edit the request or
reuse a preview after the plan, registry, repository or intended scope changes.
Then record the exact preview:

```sh
threadlane-approve record \
  --state-dir /absolute/private/runtime \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --request /absolute/private/request.json \
  --confirm-subject SUBJECT_SHA256 \
  --confirm-thread THREAD_UUID \
  --operator-approved
```

All three confirmations are required. `--confirm-subject` must be the preview's
64-character lowercase hexadecimal subject digest, `--confirm-thread` must equal
the request's canonical `thread_uuid`, and `--operator-approved` is the explicit
attestation that the operator approved this exact presented scope. Successful
stdout is the existing `ApprovalSummary` JSON document.

An exact retry of a successfully recorded request is idempotent and returns the
original summary, including its original creation time. It does not add a second
approval.

## Validation and confirmation semantics

Both commands verify the exact request binding against the live supervisor via an
authenticated `SupervisorClient::from_state_dir(...).get_binding(...)` lookup.
Prepare rechecks before returning. Record rechecks immediately before
persistence. A missing, offline or mismatched binding fails closed.

The shared authority rules still enforce:

- an active published plan revision belonging to the request binding, including
  rejection of superseded revisions;
- the current registry revision pinned by the plan, without silent refresh;
- the compiled plan digest and transitive DAG predecessor closure;
- a canonical Git repository and exact existing base commit;
- supported tools, models, efforts and canonical executable paths;
- explicit file or trailing-`/` directory write scopes covering every selected
  task;
- finite budgets and the bounded `max_parallel` concurrency limit.

The preview and the final record must agree. `approve_confirmed` rebuilds the full
actual candidate under the same in-process and file ledger lock used for
persistence. It compares the expected subject digest before any ledger write or
any existing-record success path. Changing the task selection, write scope,
budgets, concurrency, plan, registry revision, repository, base commit, binding
or thread therefore changes the subject digest; an old confirmation is rejected
even when the changed request is otherwise valid JSON. There is no auto-refresh
of a stale preview and no automatic widening or model substitution.

## Files, output and failures

Request files must be regular, absolute and private (`0600`, normally in a `0700`
directory), no larger than 1 MiB, with no symlink leaf or ancestor. Reads use
nonblocking `O_NOFOLLOW` opening plus inode/device/mode/length checks, never stdin,
and reject files that change size or identity during the read.

Successful JSON stdout is bounded to 1 MiB including its trailing newline.
Failures use a static, sanitized bounded JSON error envelope on stderr and a
nonzero exit; raw request JSON and secret sentinels are not echoed. Standard
`--help` and syntax errors use normal Clap output.

If a ledger write succeeds but the bounded stdout result cannot be written or
flushed, the command returns `outcome_unknown`. The approval may already exist:
inspect the authority ledger and retry only after reconciling the outcome. There
is no blind replay.

## Example operator sequence

```sh
umask 077
threadlane-approve prepare \
  --state-dir /absolute/private/runtime \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --request /absolute/private/intent.json \
  > /absolute/private/preview.json

# Inspect /absolute/private/preview.json. Save its `request` object as
# /absolute/private/request.json and read `subject_digest` from the same preview.

threadlane-approve record \
  --state-dir /absolute/private/runtime \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --request /absolute/private/request.json \
  --confirm-subject SUBJECT_SHA256 \
  --confirm-thread THREAD_UUID \
  --operator-approved
```

## Verification and limits

The behavioral fixture gate is
[`crates/threadlane-native/tests/approval_cli.rs`](../crates/threadlane-native/tests/approval_cli.rs).
It uses a disposable supervisor, private stores, a disposable Git repository and
a fake OpenCode executable with an invocation marker. Preparing or recording an
approval must never invoke that executable, and the fixtures make no paid provider
calls. The tests cover idempotent records, changed scope/budget/concurrency with
an old digest, malformed or mismatched confirmations, stale and superseded plans,
offline or wrong-thread bindings, invalid private request files, output failure
and the unchanged `AuthorityStore` confirmation lock.

These fixtures do not prove automatic identification of the human source of an
approval, authenticated per-call native chat identity, general multi-chat
routing, live provider behavior or macOS/Windows support. The CLI flags remain
operator attestations, not proof of conversation provenance. The existing MCP
tool surface, including its read-only approval listing and approved-task tools,
is intentionally unchanged.
