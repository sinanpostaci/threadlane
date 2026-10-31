# Conversational changes and Steer boundaries

Date: 2026-10-31. This document defines the public Rust API and `threadlane-session`
operator CLI surface for routing conversational changes while work is planned,
active, or awaiting review/integration.

## Authority and capability boundaries

- **Explicit Codex interpretation only**: Native Codex interprets the human's
  message and submits a structured [`ConversationChangeRequest`](../crates/threadlane-native/src/conversation_change.rs).
  Threadlane never listens to chat transcripts, never runs a background classifier,
  and never treats worker prose or MCP output as user authority.
- **Questions and status inquiries are side-effect-free**: A `question_or_status`
  request takes the `read_only_observation` route and produces a `record_only`
  decision (`status: "recorded_only"`, `blocks_task_advancement: false`). It never
  revises a plan, cancels a worker, consults a model, or dispatches work.
- **Distinct routes for clarifications vs. scope/model changes**:
  - `clarification` takes the `checkpoint_clarification` route (`pending_clarification_at_checkpoint`
    when held for a real workflow checkpoint). Clarifications cannot expand frozen
    write scopes or change model/effort/profile assignments; attempts to do so are
    rejected with `route_mismatch`.
  - `scope_change`, `model_or_profile_change`, and `stop_or_cancel` take the
    `scope_or_model_replan` route and return `cancel_replan_new_approval_required`
    (`status: "pending_replan"`, `replan_required: true`, `new_approval_required: true`,
    `blocks_task_advancement: true`).
- **Truthful live-input capability reporting**: Current headless worker providers
  (`antigravity`, `opencode_v2`) have no verified live in-flight input channel,
  pause, or resume proof. Every decision reports `live_input_delivered: false`,
  `process_paused: false`, `provider_resumed: false`, and
  `frozen_approval_immutable: true`. Requesting `live_continuation_if_supported`
  falls back to safe checkpoint/replan behavior with
  `live_input_status: "unsupported_no_live_input_path"`. Requesting
  `require_verified_live_input` fails closed with `unsupported_capability`.
- **Immutable approved prompts and scopes**: Frozen `ApprovalRecord` task
  instructions, model pins, and write scopes are never modified in place. New
  scope, model, or prompt changes require publishing a new plan revision and
  recording a fresh human approval through `threadlane-approve`.
- **Typed cancellation without PID killing or auto-replacement**: An optional
  `cancel_action` on a non-question change requires the exact 64-hex approval
  `subject_digest`, `operator_approved: true`, and `expected_task_revision >= 1`.
  It calls `ApprovedHostClient::cancel_task` and records whether the worker is
  `cancel_requested` (`prior_task_proven_stopped: false`) or `proven_stopped`
  (`cancelled`, `prior_task_proven_stopped: true`). It never kills raw PIDs,
  starts a replacement task, or records a new approval.
- **`cancel_requested` vs. `cancelled`**: A task in `cancel_requested` or `unknown`
  state is not proven stopped. Explicit resolution (`resolve-change`) refuses to
  resolve a change while an affected task is still `running`, `cancel_requested`,
  or `unknown`.

## Storage and concurrency model

Scoped change records are persisted under `<plans_dir>/conversation-changes/` in
per-binding private (`0600`) files (`changes-<binding_tag>.json`) protected by
in-process mutexes and cross-process exclusive `fs2` lock files
(`changes-<binding_tag>.lock`).

- Directory permissions must be `0700` and file permissions `0600`; symlinks and
  traversal components are rejected with `security_violation`.
- Each binding's ledger is bounded to `1 MiB` (`MAX_CONVERSATION_CHANGE_STORE_BYTES`)
  and `256` records (`MAX_CONVERSATION_CHANGES_PER_BINDING`).
- Every mutation enforces `expected_change_revision` compare-and-swap and
  `request_key` idempotency. Ambiguous cancellation outcomes cannot be blindly
  replayed (`ambiguous_outcome_pending_reconciliation`).

## Public Rust API (`threadlane_native`)

`crates/threadlane-native/src/conversation_change.rs` exports (and `threadlane_native`
re-exports at the crate root):

- Store & service:
  - `ConversationChangeStore` (`open`, `plans_dir`, `namespace_dir`,
    `current_revision`, `get_change`, `list_changes`, `unresolved_blocking_changes`,
    `preview_change`, `record_change`, `resolve_change`)
  - `inspect_unresolved_blocking_changes(plans_dir, binding_id, approval_id, task_id) -> Result<UnresolvedChangeBlockSummary, NativeError>`
- Request & response DTOs:
  - `ConversationChangeRequest`, `ConversationCancelAction`
  - `ConversationChangePreview`, `ConversationChangeRecord`, `ConversationChangePage`
  - `ResolveConversationChangeRequest`, `ConversationChangeResolution`
  - `ListConversationChangesParams`
  - `UnresolvedChangeBlockSummary`, `UnresolvedBlockingChange`
- Enums:
  - `ConversationChangeKind` (`QuestionOrStatus`, `Clarification`, `ScopeChange`,
    `ModelOrProfileChange`, `StopOrCancel`)
  - `ConversationChangeRoute` (`ReadOnlyObservation`, `CheckpointClarification`,
    `ScopeOrModelReplan`)
  - `ConversationChangeDecisionKind` (`RecordOnly`,
    `PendingClarificationAtCheckpoint`, `CancelReplanNewApprovalRequired`)
  - `ConversationChangeStatus` (`RecordedOnly`, `PendingClarification`,
    `PendingReplan`, `Resolved`)
  - `ConversationDeliveryRequest` (`SafeCheckpointOrReplan`,
    `LiveContinuationIfSupported`, `RequireVerifiedLiveInput`)
  - `LiveInputSupportStatus` (`NotApplicableReadOnly`, `UnsupportedNoLiveInputPath`)
  - `ObservedTaskStopState` (`NotStarted`, `Running`, `CancelRequested`,
    `ProvenStopped`, `Unknown`)
  - `ConversationChangeResolutionKind` (`LinkedPlanRevision`, `CheckpointHandled`)
- Errors & constants:
  - `ConversationChangeError`
  - `CONVERSATION_CHANGE_SCHEMA_VERSION`, `CONVERSATION_CHANGE_POLICY_VERSION`,
    `CONVERSATION_CHANGE_NAMESPACE_DIR`, `MAX_CONVERSATION_CHANGE_STORE_BYTES`,
    `MAX_CONVERSATION_CHANGES_PER_BINDING`, `DEFAULT_CONVERSATION_CHANGE_LIST_LIMIT`,
    `MAX_CONVERSATION_CHANGE_LIST_LIMIT`, `MAX_CONVERSATION_CHANGE_CURSOR_LEN`,
    `MAX_CONVERSATION_CHANGE_SUMMARY_BYTES`, `MAX_CONVERSATION_CHANGE_DETAIL_BYTES`,
    `MAX_CONVERSATION_CHANGE_RESOLUTION_NOTE_BYTES`

### Read-only blocking helper for workflow and profile-flow integration

To check whether a binding, approval, or task has unresolved blocking changes
before advancing a workflow or dispatching/reviewing work, call:

```rust
let summary = threadlane_native::inspect_unresolved_blocking_changes(
    &plans_dir,
    &binding_id,
    Some(&approval_id),
    Some(&task_id),
)?;
if summary.blocked {
    // Refuse advancing invalidated work; inspect `summary.blocking_changes`.
}
```

`question_or_status` entries never set `summary.blocked = true`. Unresolved
`pending_clarification` and `pending_replan` entries set `summary.blocked = true`
for the targeted task, any downstream dependent tasks in the approval DAG, or the
entire approval when `task_id` on the change record is `None`.

## Operator CLI commands (`threadlane-session`)

All commands require the five canonical private (`0700`) store directories plus
`--binding <ID>` and `--thread <UUID>` (either directly on `threadlane-session` or
via `threadlane-context --context /private/context.json session ...`).

### 1. Preview a conversational change (`preview-change`)

Evaluates the interpreted conversational change against the live binding,
authority record, plan store, and host session without mutating state or
cancelling tasks:

```sh
threadlane-session \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --authority-dir /private/authority \
  --sessions-dir /private/sessions --binding BINDING_ID --thread THREAD_UUID \
  preview-change --request /private/change-request.json
```

### 2. Record a conversational change (`record-change`)

Persists the scoped [`ConversationChangeRecord`](../crates/threadlane-native/src/conversation_change.rs)
with `request_key` idempotency and `expected_change_revision` checking. When
`cancel_action` is included in the request, `record-change` verifies the exact
approval `subject_digest`, `operator_approved: true`, and `expected_task_revision`
and invokes `ApprovedHostClient::cancel_task`:

```sh
threadlane-session \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --authority-dir /private/authority \
  --sessions-dir /private/sessions --binding BINDING_ID --thread THREAD_UUID \
  record-change --request /private/change-request.json
```

When `cancel_action` is present, `--confirm-approval <SHA256>` and
`--operator-approved` may also be supplied on the command line and must match the
frozen approval `subject_digest`.

#### Example `ConversationChangeRequest` JSON

```json
{
  "request_key": "chg-req-001",
  "expected_change_revision": 0,
  "approval_id": "apr_0123456789abcdef0123456789abcdef",
  "plan_id": "plan-main",
  "expected_plan_revision": 1,
  "task_id": "task-1",
  "expected_task_revision": 2,
  "kind": "scope_change",
  "summary": "Operator requested adding src/extra.rs to task-1 write scope.",
  "detail": "Hold replacement until task-1 is proven stopped and plan rev 2 is approved.",
  "proposed_write_paths": ["src/lib.rs", "src/extra.rs"],
  "requested_delivery": "safe_checkpoint_or_replan",
  "cancel_action": {
    "expected_task_revision": 2,
    "confirm_approval_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "operator_approved": true,
    "reason": "Scope changed; stop task-1 before replanning."
  }
}
```

### 3. List scoped conversational changes (`list-changes`)

Lists a bounded page (`1..=20`, default `20`) of change records for the bound
conversation, along with `unresolved_block_summary`:

```sh
threadlane-session \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --authority-dir /private/authority \
  --sessions-dir /private/sessions --binding BINDING_ID --thread THREAD_UUID \
  list-changes --approval-id APPROVAL_ID --unresolved-only --limit 20
```

Optional flags: `--approval-id <ID>`, `--task-id <ID>`, `--unresolved-only`,
`--cursor <TEXT>`, `--limit <N>`.

### 4. Resolve a pending conversational change (`resolve-change`)

Explicitly resolves a `pending_clarification` or `pending_replan` change after
verifying that any affected task is proven stopped (`not_started` or
`proven_stopped`, never `running`, `cancel_requested`, or `unknown`) and that the
linked plan revision (and optional new approval) exists and is valid:

```sh
threadlane-session \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --authority-dir /private/authority \
  --sessions-dir /private/sessions --binding BINDING_ID --thread THREAD_UUID \
  resolve-change --request /private/resolve-request.json
```

#### Example `ResolveConversationChangeRequest` JSON

```json
{
  "request_key": "chg-res-001",
  "change_id": "chg_0123456789abcdef0123456789abcdef",
  "expected_change_revision": 1,
  "resolution_kind": "linked_plan_revision",
  "target_plan_revision": 2,
  "new_approval_id": "apr_fedcba9876543210fedcba9876543210",
  "resolution_note": "Published plan revision 2 and recorded fresh approval."
}
```

## Verification and limits

Behavioral tests in [`crates/threadlane-native/tests/conversation_change.rs`](../crates/threadlane-native/tests/conversation_change.rs)
cover:

- questions and status inquiries during active worker execution without plan
  revisions, cancellations, model calls, or blocking gates;
- pending clarifications held for a real checkpoint versus rejected scope/model
  smuggling on the clarification route;
- stale expected change/plan/task revisions, idempotent request-key retries,
  conflicting request keys, and cross-binding/thread rejection;
- scope and model changes requiring replanning and fresh human approval;
- `cancel_requested` (not yet proven stopped; blocks resolution) versus
  `cancelled` (proven stopped; permits resolution with a published new plan
  revision);
- truthful handling of unsupported live provider continuation (`live_input_delivered: false`,
  `process_paused: false`, `provider_resumed: false`, and refusal of
  `require_verified_live_input`).

These commands do not establish live provider in-flight steering, transcript
monitoring, automatic replacement task dispatch, native per-call caller identity,
general multi-chat routing, or macOS/Windows support.
