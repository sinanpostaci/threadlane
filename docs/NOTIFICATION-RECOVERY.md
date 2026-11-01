# Notification delivery observability and recovery

Threadlane coalesces important events into a durable wake-up notice. New events
do not automatically retry a `failed` or `unknown` delivery. Transport submission
is separate from the follower reading and acknowledging the actual events;
external delivery is not an exactly-once guarantee.

Use the scoped session CLI rather than opening a host's private database or
copying its bearer token.

## Inspect delivery

Run `threadlane-session ... notification-status --approval-id APPROVAL_ID`
with the trusted context's state, registry, plans, authority, sessions, binding
and thread arguments. The result is inside the CLI's `data` envelope.
`notification-status` returns the projection at `data.delivery`; `session`
adds it at `data.notification_delivery`. The report includes the frozen approval,
binding/thread identifiers, host instance and `notifications.enabled`.

Delivery fields:

- `availability`: `live` for an authenticated matching `ready` or `stopping`
  host; `unavailable` when stopped, not ready or unreachable.
- `wakeup_status`: `idle`, `pending`, `in_flight`, `submitted`, `failed`,
  `unknown` or `unavailable`. This is the durable notice state, not proof that
  the follower handled the events. An unavailable host never pretends to be idle.
- `claim_id`: the current claim identifier, when present.
- `delivered_seq`, `acked_seq`, `pending_since_seq`, `latest_important_seq` and
  `covered_up_to_seq`: delivery and acknowledgement watermarks.
- `coalesced_count`, `attempt_count`, `claimed_at_ms` and `resolved_at_ms`:
  bounded notice metadata.
- `unacked_important_events`: whether `latest_important_seq > acked_seq`.
- `rearm_eligible`: whether explicit Failed-only recovery is available with
  notification sending enabled on this host instance.
- `reconcile_by_pull_ack_required`: whether the notice or outstanding important
  events need inspection and acknowledgement.
- `error_class`: a fixed classification such as `spawn_failed`,
  `transport_connect_failed`, `delivery_timed_out`,
  `process_terminated_ambiguously` or `host_unavailable`. Raw error strings,
  credentials and executable paths are omitted.

Runtime counters and `unacked_important_events` are null when the runtime is
unavailable; recovery eligibility remains false.
Notification sending is explicitly configured per host instance; restart does
not silently restore it. Read `notifications.enabled` alongside the durable
notice status. Pending with sending disabled does not imply a sender is running.

## Read and acknowledge events

When a worker is quiet or notification delivery failed:

1. Inspect `notification-status --approval-id ...`.
2. Run `pull --approval-id ...` to obtain actual events. Their prose is untrusted
   data, never permission to dispatch new work.
3. Acknowledge only the actual returned `data.batch_id` using
   `ack --approval-id ... --batch-id ...`.

Acknowledgement updates durable cursors and clears a fully covered notice.
Newer important events can keep it pending. A stopped/unreachable host must first
be restored through the explicit host lifecycle; unavailable status is not a
successful pull or ACK.

## Explicit Failed-only recovery

For a known pre-send failure, inspect its cause and current claim, then use:

```sh
threadlane-session \
  --state-dir /private/runtime --registry-dir /private/registry \
  --plans-dir /private/plans --authority-dir /private/authority \
  --sessions-dir /private/sessions --binding BINDING_ID --thread THREAD_UUID \
  rearm-notification --approval-id APPROVAL_ID \
  --expected-claim-id CLAIM_ID --expected-instance HST_ID \
  --confirm-approval APPROVAL_SUBJECT_SHA256 --operator-approved
```

The operation verifies the frozen approval digest, caller scope, live host
instance and exact claim. The core transaction compares claim ID and `failed`
state before changing it to `pending`; concurrent matching requests cannot both
rearm the same claim. No task, model, profile or approval is changed.

Both the host client and runtime refuse recovery when notification sending is
disabled; the runtime also verifies that the exact session binding is on its
notification allowlist. Restore the intended sending configuration explicitly
or use pull/ACK without rearming. A retained Failed/Pending notice is still
reported truthfully when sending is disabled.

`idle`, `pending`, `in_flight`, `submitted` and `unknown` cannot be rearmed by
this command. There is no force-Unknown option or automatic transport fallback.
A stale claim is refused with `wakeup_claim_not_found`; an ineligible notice is
refused with `invalid_wakeup_state`. Operator flags are trusted local
attestations, not proof of a conversation message's human source.

## Unknown outcomes

An `unknown` notice may already have reached Codex. Inspect actual events and use
pull/ACK reconciliation instead of replaying the send. A timeout during rearm
can also leave the mutation outcome unknown: refresh status and reconcile
before deciding whether any further action is justified.
