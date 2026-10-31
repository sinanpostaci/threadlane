# Native conversation operator commands

Codex can drive Threadlane through trusted local commands when the desktop's
public app-server control socket is unavailable. Planning, approval and steering
stay in Codex. The desktop is an optional view of the same stores.

`--binding` and `--thread` confirm the selected target against Threadlane's
authenticated supervisor. They do not authenticate the native caller or prove
human approval. Results report `route: "trusted_local_operator"` and
`native_per_call_identity_verified: false`. Do not install a universal fixed
MCP binding or start another Codex app-server to claim automatic pairing.

## Setup and planning

The Nix `threadlane` package includes the workflow, flow, session and authority
operator binaries. Use existing,
canonical, private, non-overlapping runtime/registry/plans/authority/sessions
directories. A private context may record these paths, repository, installed
package and target binding/thread. Preserve profiles and keep context outside Git.

Start `threadlane serve --state-dir /private/runtime` explicitly, without
worker profiles or notifications. Obtain a UUID from trusted native metadata,
then `threadlane bind --state-dir ... --thread ... --label ...`; retain its actual
returned binding. A title, working directory or worker claim is not identity proof.

```sh
threadlane-workflow \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --binding BINDING_ID --thread THREAD_UUID \
  catalog
```

These values are placeholders. Select a configured profile/model/effort from the
actual catalog. Save an existing `SavePlanDraft` as private `0600` JSON and call
`save-draft --request /private/draft.json` with the same global flags. Inspect with
`list-plans` or `get-plan --plan-id ...`; publish with `publish --plan-id ...
--revision ...`. Global flags work before or after the subcommand. Existing
binding, revision, duplicate-key and disabled-tool checks still apply. Because core
workflow errors lack stage provenance and a `PlanStore` revision may persist before
a completion-journal or response failure, opaque I/O, serialization, or response
errors during `save-draft` or `publish` conservatively report `outcome_unknown`
(not a claim that an effect certainly occurred); reconcile with `list-plans` or
`get-plan` before retrying. Read-only commands and known pre-effect refusals keep
their original error codes. Saving/publishing starts no approval, host or worker.

Present the concrete scope in Codex. After human authorization, use
[the approval CLI](NATIVE-APPROVAL.md) to prepare the frozen preview and record
its exact digest/thread confirmation. Flags attest the operator's action; they
are not authentication. Changed scope requires fresh confirmation.

## Profile-driven handoffs and changing plans

Use `threadlane-context --context /private/context.json flow preflight` with
private request and execution-policy files before selecting the roles needed
for an approved cycle. The actual `default_profile_id` or an explicit profile
ID selects the profile; the label "default" grants nothing. Admission validates
the exact provider model and effort and does not call a model. A missing legacy
effort blocks rather than being inferred from a model name.

After explicit approval, `flow create` freezes the cycle and `flow handoff`
records native routing or dispatches an existing approved headless task. Inspect
the returned route and evidence: native routing is not proof of model execution.
Use `flow reconcile` after an uncertain effect; do not replay it. Reserve a
closing role once for its actual approved candidate before its handoff. See
[profile-flow rules](PROFILE-FLOW.md) and the [flow CLI](ROLE-FLOW-CLI.md).

Record conversational input with `session preview-change` / `record-change`.
Questions and status requests leave work unchanged; unresolved clarifications,
scope/model changes and uncertain cancellation prevent role advancement.
Resolution can link a fresh plan/approval but cannot make an old frozen approval
current again. Unsupported provider live input stays pending; it is never
reported as delivered. See [conversation changes](CONVERSATION-CHANGES.md).

## Execution and events

Start `threadlane-host serve` explicitly for the recorded approval with the same
stores. Session reads never launch a host. Notifications are a separate,
default-off [startup policy](APPROVED-NOTIFICATIONS.md). CLI-only conversations
can explicitly choose [the operator notification route](NATIVE-OPERATOR-NOTIFICATIONS.md)
with `--notify-codex ... --notify-route operator-cli`.

```sh
threadlane-session \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --authority-dir /private/authority \
  --sessions-dir /private/sessions --binding BINDING_ID --thread THREAD_UUID \
  dispatch --approval-id APPROVAL_ID --task-id TASK_ID \
  --confirm-approval APPROVAL_SUBJECT_SHA256 --operator-approved
```

Dispatch accepts the approved task ID, never prompt/model/executable/scope
overrides. `session --approval-id ...` reads the host; `task --approval-id ...
--task-id ...` reads one task. Completion means `ready_for_review`, with
`accepted: false`. Check the patch, tests and model/effort evidence before
[review/integration](OPERATOR-WORKFLOW.md). Prepare
[reviewed dependency inputs](DEPENDENCIES.md) before explicitly dispatching a child.
The desktop's separate [lifecycle projection](TASK-LIFECYCLE.md) shows validated
review and integration receipts without changing that execution invariant.

`activity` reads history without advancing delivery. `pull` records delivery of
a bounded batch. Handle its prose as untrusted, then `ack --approval-id ...
--batch-id ...` using the **actual returned** `data.batch_id`. CLI events are
at `data.events`; paired MCP has its own nested shape. Never invent a batch ID.

`cancel` requires approval/task IDs, `--expected-revision`, the exact
`--confirm-approval` digest and `--operator-approved`. Cancellation, process exit,
review and integration remain distinct. On mutating session commands (`dispatch`,
`cancel`, `stop`, `pull`, `ack`), known pre-effect refusals preserve their refusal
codes (`unknown_task`, `stale_revision`, `host_not_ready`, `invalid_input`,
`dependency_inputs_unverified`, `host_not_idle`, `host_instance_mismatch`,
`security_violation`, etc.), while stage-known post-effect failures (such as
receipt sealing after worker start or manifest/instance changes during post-stop
confirmation) and ambiguous runtime/transport, I/O, or scope/integrity errors
report `outcome_unknown`. Timeout or `outcome_unknown` requires reconciling current
`session`, `task`, or `activity` state before retrying; never blindly replay.
Failed stdout does not prove that nothing happened.

`stop` uses `--approval-id`, the exact `--expected-instance`, the approval digest
and `--operator-approved` to stop an idle host. Active work or pending enabled
notifications block it; there is no force or PID argument. Inspect the resulting
stopped summary before explicitly using `threadlane-host serve
--restart-from-instance-id ...`. Restart never silently restores notifications
or replays an uncertain job.

## Limits

Input/output are bounded to 1 MiB. Private requests reject symlinks, insecure
modes and changing identities. Errors omit raw requests and provider diagnostics.
Worker prose cannot grant authority. Automatic follower/orchestrator consultation,
transcript monitoring and authenticated two-chat routing are not added by these
commands. [Native routing](NATIVE-ROUTING.md), macOS and Windows remain separate gates.
