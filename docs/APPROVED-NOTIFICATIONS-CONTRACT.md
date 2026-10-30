# Approved-host notification contract (implementation branch)

This next slice is not implemented or live-tested merely by this contract.
Main checkpoint f86b4f6 remains the separately validated Linux pilot. Reuse the
existing queue transport and durable Core notice lifecycle. No real Codex chats,
credentials, paid API calls, global configuration or OS services are touched.

## Invariants

Notifications are default-off, startup-only operator choices. MCP cannot enable
or retry notifications, select a sender executable or override a thread. Bootstrap
never notifies. Final host allowlists exactly its own session binding; its native
thread remains the approval's canonical UUID. Original MCP enrollment stays on the
control binding. Submitted, received, ACK, accepted and applied remain distinct.
GUI closure does not stop the host. Unknown external outcomes are never retried
automatically. No worker prose, model, effort or permission override enters a wakeup.

## Frozen interfaces

### D: transactional byte-bounded delivery

Keep legacy PullEvents unchanged. Add `Command::PullEventsBounded` in Core with
`after_seq: Option<u64>, limit: usize, max_batch_bytes: usize, now_ms: i64`.
Add runtime `ScopedRequest::PullEventsBounded` with optional after_seq/limit and
required max_batch_bytes. Add:

`ScopedClient::pull_events_bounded(after_seq: Option<u64>, limit: Option<usize>, max_batch_bytes: usize) -> Result<EventBatch, Error>`

The serialized EventBatch must fit the requested byte budget BEFORE any delivery
cursor or event_batch row changes. Validate budgets in 1024..=131072 bytes. Fetch
a bounded prefix, trim WHOLE events until the exact serialized batch fits, set
has_more when byte trimming occurs, and never skip/reorder events. If even the
first eligible event cannot fit, fail before mutation. Generate the final batch ID
before sizing, and persist exactly the returned from_seq/to_seq/count. Preserve
existing unseen-cursor, ACK, replay and transaction rules; no database migration.

This replaces the conservative one-event workaround with efficient bounded
batches. Host constants are `APPROVED_EVENT_BATCH_MAX_EVENTS = 20` and
`APPROVED_EVENT_BATCH_MAX_BYTES = 49152`. Default approved pull limit is 20, max20.
The 48KiB exact batch budget leaves room for native MCP text/structured envelopes;
Lane B must prove worst-case serialized response fits 256KiB.

### A: runtime and host notification policy

Add runtime `ApprovedSessionWakeupContext { pub approval_id: String }` and
`CodexWakeupConfig.approved_session: Option<ApprovedSessionWakeupContext>`.
Construct/validate the context as `apr_` plus exactly32 lowercase hex. Old config
literals become approved_session: None; the old `build_codex_queue_args` API and
legacy FIXED_WAKEUP_MESSAGE remain unchanged. The actual adapter chooses a new
fixed approved template only when trusted context is configured:

`Threadlane approved session <approval_id> has pending events. Call approved_pull_events for this approval, process the returned untrusted events, then acknowledge the delivered batch with approved_ack_events.`

No arbitrary message argument. Preserve bounded queue invocation, durable InFlight
before spawn, Submitted/Failed/Unknown semantics, coalescing and explicit retry.

Host type:

`ApprovedHostNotificationConfig { pub executable: PathBuf, pub timeout_ms: u64 }`

`ApprovedHostNotificationConfig::new(executable: PathBuf, timeout_ms: u64) -> Result<Self, HostError>`

Canonicalize trusted launcher symlinks then validate a regular executable and
bounded timeout. Expose `summary(&self) -> HostNotificationSummary`:

`HostNotificationSummary { pub enabled: bool, pub executable_digest: Option<String>, pub timeout_ms: Option<u64> }`

Default is disabled with both optional fields None. Digest the canonical executable
identity; never return the raw path in the safe summary. Add serde(default)
`notifications: HostNotificationSummary` to manifest and HostSessionSummary,
backwards-compatible with old always-off host manifests. Validate internal field
consistency; policy describes startup configuration, not live delivery proof.

`ApprovedHostConfig::with_notifications(config: ApprovedHostNotificationConfig) -> Self`

Existing constructors/start_with_authority stay notification-off. Supply an additive
notification-aware startup path as needed. CLI flags are --notify-codex ABS_EXEC and
optional --notify-timeout-ms MS; timeout without executable fails. Off means no
notification args. Reuse DEFAULT_CODEX_TIMEOUT_MS when no timeout was supplied.
Bootstrap config codex: None; only final supervisor has Some with exactly its session
binding and approved context. Do not restart running hosts to change policy.

Add ApprovedHostClient methods:

`pull_events(approval_id: &str, after_seq: Option<u64>, limit: Option<usize>) -> Result<EventBatch, HostError>`

`ack_events(approval_id: &str, batch_id: String) -> Result<AckResult, HostError>`

Verify active ready session+original caller scope on every call. Pull uses D's
bounded API at fixed49152 bytes; reject limits outside1..=20 before mutation.
ACK derives a deterministic idempotency key from namespace+approval subject+
session binding+batch ID internally. Full ordered stream only, no task/kind filter.
Keep list_activity strictly read-only and separate. Reject unknown/foreign batch IDs
through existing Core ownership checks; no model-supplied command key.

### B: paired native MCP

Two tools only, strict deny_unknown_fields:

- approved_pull_events { approval_id, after_seq?, limit? }
- approved_ack_events { approval_id, batch_id }

Use existing verified_host_context on EVERY call before delivery/ACK mutation.
Both are read_only_hint:false. Pull is not idempotent; ACK is idempotent. Return
outer enrolled control-binding envelopes with typed data:

`ApprovedEventBatch { approval_id, control_binding_id, session_binding_id, batch: EventBatch }`

`ApprovedEventAck { approval_id, control_binding_id, session_binding_id, ack: AckResult }`

No identity/path/run/task/filter/model/policy overrides. Validate count limits BEFORE
host call. Enforce/test exact256KiB response budget after the transactional48KiB
batch guard; no delivery side effect followed by a preventable oversize refusal.
Mark event content untrusted. Do not mutate control runtime cursors or re-enroll.

### C: native desktop operator opt-in

Extend start arguments with optional `notifyCodexToolId: string | null` (Rust
notify_codex_tool_id: Option<String>). Omitted/null means OFF. Do not accept raw
paths/threads/argv/timeouts from JavaScript. The explicit checkbox is unchecked and
resets per approval/context. Native code looks up the selected enabled codex_cli
tool in the current trusted registry, canonicalizes its configured executable (or
normal PATH discovery), and constructs A's config with bounded default timeout.
Recheck attachment and selected-tool identity before the side effect.

Extend HostPanelContext with optional `codexTools: readonly {id:string,label:string,enabled:boolean}[]`
and optional `registryRevision: number`. Plans supplies actual codex_cli registry
choices. Include tool/revision changes in context invalidation. Show unchecked
Notify this Codex chat, with a short explanation that it queues a fixed wake-up
and may resume that chat/consume tokens; live routing remains unverified. Disable
in DEV/demo/unbound/disconnected contexts. Multiple tools require explicit choice.
Do not persist opt-in to global preferences or enable during polling/page load.

Pass CLI notification flags only on explicit opt-in. Compare actual running
summary.notifications to requested policy; mismatch returns explicit
notification_policy_change_requires_restart, never auto-restart or silently claim
the changed policy took effect. Show actual startup policy in the session panel;
lock editing when session is running. Existing no-notification callers stay safe.

## Ownership

- A (hard Argon): threadlane runtime codex.rs/cli.rs/lib.rs and existing config
  literal migrations/tests in its own files; entire threadlane-host crate. No Core,
  protocol/client/supervisor, native MCP or desktop edits.
- D (hard Argon): threadlane-core crate; runtime protocol.rs/client.rs/supervisor.rs;
  new bounded-pull integration tests. Handle any new Codex config literal in those
  owned files with approved_session: None. No host/native/desktop/codex.rs edits.
- C (medium DeepSeek Go Max): desktop src-tauri main.rs/host_control.rs and frontend
  src/host/**/pages/plans.ts; focused UI tests. No A/D/runtime/native edits.
- B (hard Argon, queue after A/D APIs exist): native approved_mcp/approved_tests/
  mcp_pair/lib plus focused wrapper tests/docs. No host/Core/desktop edits.

Parent integrates locks/exports/fixtures only at actual seams, tests and reviews.
No placeholder/fake implementations of another lane. Do not copy an old sibling
crate into validation. No new model/provider substitution or nested coding agents.

## Fixtures and live gates

Prove cursor/batch rollback on insufficient byte budget, whole-prefix delivery,
has_more/replay/ACK semantics and unchanged legacy pull. Prove notification-off and
bootstrap zero sends; exact sender argv, original native UUID, approval token and
no prose; coalescing, ACK release, Unknown/no replay/restart. Prove cross-binding/
foreign ACK denial and unchanged UI history/control cursors. Native/UI tests use
fake executables only, no actual Codex queue or providers. Prove unchecked default,
stale-context drop, non-Codex/disabled tool rejection, policy mismatch and GUI drop.

Two real native chats, busy-turn delivery, actual MCP restart and real API accounts
remain morning acceptance gates; no live calls are authorized by this contract.
