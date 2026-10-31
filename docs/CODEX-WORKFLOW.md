# Working from the native conversation

Native Codex is the everyday conversation surface. The user plans, asks
questions, approves the intended work and changes direction there. The Codex
follower translates those decisions into explicit, versioned MCP requests.
Threadlane supervises validated execution and retains correlated progress and
results. The desktop window is an optional view for settings, observation and
intervention; it is not intended to be a per-task form.

Threadlane does not monitor keystrokes or silently interpret the conversation.
The follower submits the relevant decision through MCP. A question does not
change a published plan. A changed scope or model assignment requires a new
validated revision and its appropriate trusted approval.

## Setup and authority

Tools and models are configured globally, while profiles assign the permitted
models and effort levels to roles and difficulty. Pairing connects one native
MCP connection to its original chat. Operator approval freezes an execution
scope, including repository/base, tasks, models and write paths. A task argument,
worker response or ordinary publication does not grant new authority.

Initial pairing, profile configuration and approval of a new scope are distinct
setup/authority steps. Tasks inside an existing approval can be dispatched and
observed through MCP without another desktop visit. Creating a new trusted
approval uses either explicit desktop confirmation or the separate operator-only
`threadlane-approve` CLI described in [native approval](NATIVE-APPROVAL.md). The
CLI can prepare and record an exact scope, but its confirmation flags are operator
attestations rather than authentication; they do not prove human conversation
provenance. A model-generated claim that the user approved something must not
grant execution authority, and MCP exposes no self-approval tool.

## Headless scope approval

`threadlane-approve prepare` renders the exact frozen task model/effort/CLI pins,
write scopes, instructions and budgets for an `OperatorApprovalIntent`. It does
not persist an approval. The trusted operator inspects that preview, saves its
`request` object privately, and runs `record` with the preview subject digest,
the request thread UUID and the explicit `--operator-approved` attestation.
Changed plans, registries, repositories, scopes, budgets or concurrency require a
fresh preview and confirmation.

The route rechecks the exact `binding_id` and canonical `thread_uuid` against the
live configured supervisor before returning the preview and immediately before
persistence. Offline or mismatched bindings fail closed. Recording an approval
does not start the task, create a worktree, call a provider, merge or push.

## Initial host startup

The optional operator setting `--auto-start-approved-hosts` on
`threadlane-native-mcp` enables first startup from an explicit
`start_approved_task(approval_id, task_id)` call. It requires the configured
registry, plans, authority and sessions stores. The executable is the matching
`threadlane-host` sibling, or a validated `--host-executable` supplied by the
operator. MCP arguments never select executable paths or arbitrary commands.

The launcher verifies the current enrollment and frozen root task, starts a
fresh host, waits for authenticated readiness, rechecks enrollment, and then
uses the existing idempotent task dispatch. Read-only calls never launch it.
The setting defaults off for existing installations and library callers.
Notifications remain a separate explicit startup choice; initial auto-start
does not enable them.

For an already configured Unix installation, the operator's stdio registration
can use the matching packaged binary and its existing private stores:

```sh
/absolute/package/bin/threadlane-native-mcp \
  --state-dir /absolute/private/runtime \
  --pairing-dir /absolute/private/pairing \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --sessions-dir /absolute/private/sessions \
  --auto-start-approved-hosts
```

These are placeholders, not a setup command that enrolls a chat or creates
approval. Store paths must be distinct, canonical and private. An explicit
operator override may add `--host-executable /absolute/package/bin/threadlane-host`;
models cannot change that startup policy through MCP.

Closing the desktop leaves separately owned approval hosts running. Existing
Ready hosts can be attached after authentication. Stopped, failed, orphaned or
uncertain sessions are not automatically restarted. A timeout is an uncertain
outcome to reconcile, not permission to replay an external effect.

An approved host initializes and verifies its repository-bound work-area marker
and directories before becoming Ready. Concurrent first tasks therefore observe
a fully initialized work area; worker startup does not establish its ownership
for the first time after readiness has already been advertised.

## Progress and changes of direction

The follower reads compact task status and bounded event batches through MCP,
then acknowledges the actual returned batch after handling it. Event prose is
untrusted data. Notification routing uses software-owned approval/chat identity,
and delivery remains distinct from the follower handling the result.

The user can keep steering the conversation while work is active. Changes that
affect active work must use the appropriate revision/cancel/reconcile boundary;
new scope must not be injected into an old worker implicitly. Worker completion
means ready for review, with `accepted: false`; it does not merge or apply code.

## Current acceptance limits

The lifecycle package on 667d975 passed its documented Linux fixture and build
gates. This startup extension has its own validation gate. Real two-chat routing,
busy-turn delivery, live interactive provider continuation, receipt-bound DAG
inputs, API entitlement and macOS/Windows release behavior remain separate.
Local demos or a submitted CLI call do not establish those outcomes. See
[validation](VALIDATION.md), [native routing](NATIVE-ROUTING.md) and
[session lifecycle](SESSION-LIFECYCLE.md).
