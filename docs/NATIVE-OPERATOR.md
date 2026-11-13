# Native conversation operator commands

Codex can drive Threadlane through trusted local commands when the desktop's
public app-server control socket is unavailable. Planning, approval and steering
stay in Codex. The desktop is an optional view of the same stores.

`--binding` and `--thread` confirm the selected target against Threadlane's
authenticated supervisor. They do not authenticate the native caller or prove
human approval. Results report `route: "trusted_local_operator"` and
`native_per_call_identity_verified: false`. Do not install a universal fixed
MCP binding or start another Codex app-server to claim automatic pairing.

## Startup guide and compaction recovery

Run `threadlane guide` to print the canonical versioned operating guide
([`docs/MCP-OPERATING-GUIDE.md`](MCP-OPERATING-GUIDE.md)) offline without network
or supervisor access. When connected over MCP (`threadlane mcp` or enrolled
`threadlane-native-mcp`), call the read-only `operating_guide` (`{}`) tool to
fetch the same shared guide and version (`2026-11-13.v1`).

Compaction detection and memory retention are not automatic. On initial
connection, session resume, context uncertainty, or before external effects when
rules or state are uncertain, refetch `operating_guide` (or run `threadlane guide`
on the trusted-local CLI route) and then refresh live scoped state and revisions
(`status`, bounded `backlog_list` / `backlog_get` / `backlog_history`, and only
actual recorded execution references when invoking guarded
`backlog_worker_result` or `backlog_worker_reject`). Read `list_approvals`,
`approved_session`, and `approved_task` when their actual IDs are known. Legacy
jobs additionally use `coworker_status`, `list_plans`, and `get_plan`. Do not
invent identifiers or use a polling loop.

## Setup and planning

For ordinary new work, use the per-chat backlog and a concrete single-task
scope. Create or update a backlog item, present the exact scope, and obtain one
explicit human authorization. Within that unchanged scope, an operator may
publish the required technical plan record directly with `save-draft` and
`publish` (no model `Orchestrator` or `prepare_work` is mandatory), record the
exact authority, and launch the Worker without redundant approval ceremonies.
Backlog creation or editing is not authority, and a local flag remains an
attestation, not authentication. Changed scope, model, effort, quota,
repository, base, or task identity needs a fresh concrete preview. Real Worker
execution still requires a frozen execution profile and registry revision plus
exact model/effort/canonical pins in the authority; a Coworker-only backlog item
has no execution profile. Only the multi-role stages are optional.

On the managed Linux Codex desktop route, call the no-argument
`connect_current_chat` MCP tool on first use, then `operating_guide`. It verifies
the current MCP process's parent against the existing private desktop endpoint,
cross-checks the request's thread hint against that thread's connection identity,
and reuses or creates the durable binding. When all five private stores and the
canonical same-package `threadlane-context` binary are configured and validated,
`connect_current_chat` also exports a private `0600` per-chat operator context
under `<pairing_dir>/operator-contexts/` and returns additive `operator_context`
metadata (`available: true`, `context_path`, `context_router_executable`,
`route: "trusted_local_operator"`, `native_per_call_identity_verified: false`;
or `available: false` with `code` and `message` when unconfigured).

### Optional legacy complete-job admission

The following planning-grant and complete-job admission sequence is not required
for ordinary backlog work. Use it only when the human explicitly selects a
legacy multi-role job or continues an existing one. Old jobs remain readable
under their original frozen contracts.

On a fresh chat without an active planning grant or execution approval:

1. Before any planning model use, run `<context_router_executable> --context <context_path> flow grant-prepare --request <private_json>` and present the concrete planning permission preview to the human.
2. Only after explicit human approval of that preview in chat, record the grant via `<context_router_executable> --context <context_path> flow grant-record --request <private_json> --confirm-preview <PREVIEW_SHA256> --operator-approved`.
3. Request the actual `Orchestrator` technical plan (`prepare_work`, `request_technical_plan`, `planning_command`). Obtain the exact authority approval for its recorded published planning result: run `<context_router_executable> --context <context_path> approve prepare --request <private_json>`, present the concrete Worker scope/models/efforts/pins, repository/base, budgets, concurrency and `subject_digest`, and record that unchanged authority request only after exact human approval using `approve record --request <private_json> --confirm-subject <SUBJECT_SHA256> --confirm-thread <THREAD_UUID> --operator-approved`. Authority record starts no execution and does not imply downstream controller powers.
4. Before recording admission consent, run `<context_router_executable> --context <context_path> flow coworker-admission preview --request <private_admission_json>`. The request uses the exact existing approval ID, original job/ledger revisions, recorded planning result, execution policy, explicit capabilities, complete envelope and stable key/timestamp. Present ALL roles (configured Orchestrator/Architect and their separate planning authority, every Worker, Reviewer, Closure), resolved model/effort/canonical executable pins, dependencies/interfaces/write scope, repository/base/tree, concurrency and role/runtime/output budgets, checks, correction/attempt caps, and commit/integration powers. Distinguish configuration from historical role calls. Worker-only consent does not imply Reviewer, Closure, checks, corrections or Git.
5. Only after the complete unchanged preview is confirmed, run `<context_router_executable> --context <context_path> flow coworker-admission record --request <private_admission_json> --operator-approved --confirm-preview <PREVIEW_SHA256> --confirm-approval <APPROVAL_SUBJECT_SHA256> --confirm-envelope <ENVELOPE_SHA256>`. An identical already-approved complete scope needs no redundant consent; any changed/expanded scope needs fresh preview and consent. JSON self-attestation alone grants nothing. Preserve the original request key, creation timestamp and source revisions; the returned `job_revision` is the original resulting linked revision, not a revision to copy back into the request.
6. Inspect with `flow coworker-admission inspect --job-id <JOB_ID>` and read current MCP `preview_execution` readiness, then separately call `start_execution` with the returned frozen digest as `expected_envelope_digest`. Recording starts no host/provider/check/Git operation. Use explicit `flow coworker-admission reconcile --job-id <JOB_ID> --request-key <ORIGINAL_KEY>` after inspecting partial/uncertain persistence, never a replacement key or blind replay. The [admission reference](COWORKER-APPROVAL-ADMISSION.md) has full nested request/response shapes and the distinction between pure inspection and execution-time durability establishment.

There is no MCP self-approval tool. Local flags attest the already-approved scope,
not cryptographic human identity or native per-call provenance. When
`operator_context.available` is false, report its actual code/message and required
store/package configuration; do not invent a context or authority.

### Local loopback access and sandboxed execution

The trusted-local operator CLI (`context_router_executable`) verifies the active
binding against the local supervisor over loopback independently of the MCP
transport. If an operator command returns `transport_unavailable` after
`connect_current_chat` succeeded with `operator_context.available: true`, do not
report the supervisor as offline without checking local loopback access:

- Run one read-only `<context_router_executable> --context <context_path> flow grant-list`
  diagnostic using the same returned `context_router_executable` and
  `context_path`.
- When a command sandbox blocks local loopback, request narrowly scoped local
  execution permission for that command via the executor tool's supported
  mechanism. Never disable sandboxing globally, alter firewall/system settings,
  install fixed bindings, edit global configuration, or restart the backend.
- Keep executor permission distinct from Threadlane human preview approval.
  Never run `grant-record`, `approve record`, `coworker-admission record`/`reconcile`, `coworker-execution record`,
  writes, or model calls automatically on transport failure. After the route is
  confirmed, `flow grant-prepare` and `approve prepare` remain read-only preview
  commands; explicit human consent to the shown digest still precedes
  `flow grant-record` or `approve record`.
- Retry only a read-only diagnostic or preview once after permission is granted
  (no loops). Never replay an effectful or `outcome_unknown` command; reconcile
  durable receipts and live status first.
- If local execution permission is unavailable or declined, state that local
  operator access remains unverified (mentioning a sandbox restriction only
  when independently confirmed) rather than claiming the supervisor is offline
  or fabricating a manual substitute for an actual `Orchestrator` plan.

It may start the baseline control
supervisor with no workers or notifications; approved hosts are not resumed.
Later scoped MCP calls must carry the same host-provided thread ID. Missing or
conflicting IDs are refused. This local route trusts the OS user and verified
parent app-server; it does not claim cryptographic per-call identity. The CLI
operator path below remains available for unsupported hosts. Automatic binding
does not grant task execution approval.

The Nix `threadlane` package includes the workflow, flow, session and authority
operator binaries. Use existing,
canonical, private, non-overlapping runtime/registry/plans/authority/sessions
directories. A private context may record these paths, repository, installed
package and target binding/thread. Preserve profiles and keep context outside Git.

Start `threadlane serve --state-dir /private/runtime` explicitly, without
worker profiles or notifications. Obtain a UUID from trusted native metadata,
then `threadlane bind --state-dir ... --thread ... --label ...`; retain its actual
returned binding. For idempotent trusted-operator binding resolution after
separate native identity validation, `SupervisorClient::get_or_create_binding`
(`POST /v1/bindings/get-or-create`) returns an existing single binding unchanged
(`created: false`), inserts a new binding and wake-up record when none exists
(`created: true`), or fails closed with `ambiguous_thread_binding` if multiple
bindings already exist for that thread UUID; it is never exposed over scoped MCP
and does not perform automatic pairing. A title, working directory or worker
claim is not identity proof.

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

## Optional legacy multi-role profile-driven handoffs and changing plans

This role-cycle route is explicit opt-in legacy behavior. It is not required for
ordinary backlog work. Use `threadlane-context --context /private/context.json flow preflight` with
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

## Optional legacy automatic job execution and historical/manual routes

This route is explicit opt-in legacy behavior, not a mandatory stage for ordinary
backlog work. Old jobs and manual approved-session routes remain readable under
their original contracts. Complete admission supplies the implemented controller
driver/gateway. After
separate `start_execution`, exact current frozen authorization can drive Workers,
checks, Reviewer, bounded corrections, local integration and one accepted Closure.
Use `execution_mode`, `pipeline_status` and `pipeline_report`; initial Worker
completion or `AllTasksIntegrated` alone is not package completion. Fixture
coverage is separate from installed-package/live-provider/original-chat acceptance.

Admission preview/inspect are pure and do not sync or repair. A fresh inspection
may report `state: "uncertain"` for `receipt.stage: "complete"` without a local
witness. Deliberate execution gates may sync unchanged existing Complete
persistence and reread it to establish durability without changing bytes/mtime,
consent, revisions, counters or leases. Stored Uncertain/Intent/Linked/RuntimeRecorded
still requires original-key reconcile; start never finishes incomplete admission.
Historical inspection after plan supersession is not current execution eligibility.

Do not mutate an old job/linkage/envelope to add powers. Conflicts require a new
job or supported amendment/replan with exact reapproval and complete admission.
An already matching manual linkage may opt in only if all immutable subjects
match. New-admission Workers, Reviewer and Closure use canonical pins without
PATH fallback; unmarked manual/legacy jobs retain their previous valid routes.
The separate `flow coworker-execution preview|record|inspect` route remains
available for manual envelope recording with its original two-digest confirmations.
Older limited scopes may expose only diagnostic Phase 1 behavior.

### External resolution receipts

`flow external-resolution preview|record|inspect` preserves operator-attested
evidence that existing work was resolved outside the controller. Preview and
inspect are pure; neither creates observation journal entries, locks, caches, or
provider/Git/host effects. Record requires the independent
`--operator-approved`, `--confirm-thread`, and `--confirm-preview` flags; JSON
self-attestation grants nothing. See
[external resolution](EXTERNAL-RESOLUTION.md) for the exact request, scope,
durability, and guard rules.

A full-job receipt closes job advancement. A task-scoped receipt blocks its
covered task IDs while unrelated same-job progress may continue. Receipts are not
Review/Integration/Closure acceptance or a dependency seal, and a stale or
`persistence_uncertain` receipt never silently reopens covered dispatch. Inspect
first, then use `flow external-resolution record --reconcile-existing` only with
the original `request_key` and preview digest to sync unchanged existing bytes;
there is no automatic reconcile or new-key replay. The same read-only receipt
guard covers direct `session dispatch`/MCP `start_approved_task`,
`preview_execution`/`start_execution`, driver `ensure`/`run-owner`, controller
admission/step, and task outbox reservation. Use a disposable isolated store for
record/reopen compatibility checks; an older MCP reader rejects a non-empty
`external_resolutions` ledger.

Larger runtime policies, decomposition rules, user-language role outputs with
English code/comments, startup ergonomics, native UI status and observation
performance remain follow-up work. Admission does not fix generic dispatch/change
races or the missing-check-executable status/stop issue, nor prove power-loss safety.

## Execution and events (manual approved-session route)

The following existing approved-session commands remain available. They describe
manual task dispatch and its review/integration limits, distinct from the complete
automatic job route above.

Start `threadlane-host serve` explicitly for the recorded approval with the same
stores. Session reads never launch a host. For raw and manual callers,
notifications remain a separate, default-off
[startup policy](APPROVED-NOTIFICATIONS.md). The standard owned
managed MCP registration opts into fresh-only approved-host auto-start and managed
native notices together; an eligible initial direct `start_approved_task` then
attaches a fixed notice to the exact verified managed chat before spawn. Raw CLI,
manual and library defaults stay off. Already-running hosts keep their recorded
policy, and stopped, failed, orphaned, unknown or non-fresh sessions are never
resurrected. A notice is not approval, acceptance or proof of human reading.
Fixture coverage does not establish live native-chat delivery or reboot recovery.
CLI-only conversations can explicitly choose
[the operator notification route](NATIVE-OPERATOR-NOTIFICATIONS.md)
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
overrides. Both `threadlane-session dispatch` and paired MCP `start_approved_task`
acquire the binding's conversation advancement guard and inspect unresolved
conversational changes (`blocked_by_conversation_change`) and uncertain
cancellation markers (`ambiguous_outcome_pending_reconciliation`) before any
host auto-start, plan materialization, dispatch intent, or worker spawn, and
hold the guard until the dispatch receipt is sealed. Low-level `threadlane-host`
CLI commands (`serve`, `prepare-dependencies`) and raw `ApprovedHostClient::dispatch_task`
live in `threadlane-host` below `threadlane-native` and are not gated by the
native conversation-change ledger directly (to avoid crate dependency cycles and
reentrant lock deadlocks with `role_flow`). `session --approval-id ...` reads the host; `task --approval-id ...
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
ACK is delivery bookkeeping, not task or Worker completion. On the paired backlog
route, reconcile the actual execution and complete the item separately; never
report `Done` from an ACK.

`session --approval-id ...` includes bounded, path-free `notification_delivery`
status, and `notification-status --approval-id ...` returns a dedicated report
covering live wakeup status (`idle`, `pending`, `in_flight`, `submitted`,
`failed`, `unknown`, or `unavailable`), `claim_id`, sanitized `error_class`, and
pending/acknowledged event cursors. On user status requests, check live
delivery state (`session` or `notification-status`) and `pull` / `ack` any
pending batches rather than relying on wake-up notices alone. If a wake-up
failed pre-send (`status: "failed"`), explicitly rearm it with
`rearm-notification --approval-id ... --expected-claim-id ...
--confirm-approval ... --operator-approved` (and optional `--expected-instance`).
`unknown` wakeups are refused by `rearm-notification` and must be reconciled via
`pull` and `ack` first. See [notification recovery](NOTIFICATION-RECOVERY.md).

`cancel` requires approval/task IDs, `--expected-revision`, the exact
`--confirm-approval` digest and `--operator-approved`. Cancellation, process exit,
review and integration remain distinct. On mutating session commands (`dispatch`,
`cancel`, `stop`, `pull`, `ack`, `rearm-notification`), known pre-effect refusals preserve their refusal
codes (`unknown_task`, `stale_revision`, `host_not_ready`, `invalid_input`,
`dependency_inputs_unverified`, `host_not_idle`, `host_instance_mismatch`,
`wakeup_claim_not_found`, `invalid_wakeup_state`, `security_violation`, etc.), while stage-known post-effect failures (such as
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
