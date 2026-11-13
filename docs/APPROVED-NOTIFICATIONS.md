# Approved-session notifications

This extension connects a separately owned approval host to its original native
Codex chat. Notifications are **off by default** and are an explicit choice when
starting a session. Local fake-process coverage is separate from live native
delivery acceptance; a configured policy or successful CLI submission alone does
not prove the intended chat received or processed a message.

## Operator flow

1. Configure an enabled `codex_cli` tool in **Tools**, using a trusted local Codex
   executable. Nix launcher symlinks are resolved to their canonical executable.
2. Publish and approve a plan for the exact connected native chat.
3. Before **Start local session**, select **Notify this Codex chat**. If several
   Codex tools are available, choose one explicitly. The checkbox starts unchecked
   and resets when the approval, chat or tool registry changes. It is disabled in
   browser previews, demos and disconnected contexts.
4. Start the host. The panel shows its actual startup policy. This is a policy
   summary, not a delivery receipt. The choice is locked for a running host;
   changing it requires an explicit host restart, never an automatic restart of
   active work. The current panel does not offer host-stop/restart controls.

Opting in can resume the original Codex chat and consume tokens. It does not
select or override that chat's model, effort or permissions. Keep it off until
the intended native chat exposes the new paired MCP tools.

The standard owned managed desktop registration also opts into fresh-only
approved-host auto-start and managed native notices together. When both are
enabled on a verified managed session, an eligible initial direct
`start_approved_task` attaches this fixed native notice transport before
spawning its fresh host. The target is derived only from the enrolled scope, the
independently verified observation/managed parent and the frozen approval
repository/thread; MCP arguments cannot supply socket, executable, model,
effort or notification overrides, and no legacy queue or shell fallback is used.
Auto-start or notices alone do not enable this path. Existing hosts keep their
recorded policy, and stopped, failed, orphaned, unknown or non-fresh sessions are
never resurrected. A known notice-setup refusal before spawn is reported as a
pre-effect error, not as `outcome_unknown`, and does not create a launch intent.

Trusted CLI operators can supply `--notify-codex /absolute/trusted/executable`
and optional `--notify-timeout-ms` to `threadlane-host serve`. Omit both for off.
The timeout defaults to 10,000 ms and must be in 1..=300,000 ms. A timeout without
an executable is rejected. JavaScript accepts a registry tool ID only, never an
executable path, thread ID or arbitrary command arguments.

## Follower flow

The final host supervisor allowlists only its own session binding. Bootstrap
sends nothing. Important events coalesce into the existing durable wake-up
notice rather than producing a message for every progress line. The queued
message is a fixed template containing only the validated approval identifier;
worker output, instructions, answers and model settings never enter it.

The paired native follower uses:

- `approved_pull_events`: reads the full ordered session event stream, default
  and maximum 20 events, with a 48 KiB exact serialized batch limit. The database
  advances delivery only after the complete response batch fits. An event too
  large for the budget fails before delivery or batch-row mutation.
- `approved_ack_events`: acknowledges a returned batch after handling its
  untrusted contents. The host derives an idempotency key internally. Duplicate
  ACKs are safe; unknown or foreign batches fail closed.

These tools preserve the original enrollment on the control binding. They return
both control and host-session identities explicitly; they do not re-enroll the
chat or change its control event cursor. The Activity/History UI only observes
events and never consumes delivery or sends ACKs.

The paired MCP surface has 29 tools with this extension. Existing sessions may
need an MCP connection refresh or a newly authorized test chat to discover added
tools; do not assume an already open chat has loaded them.

## Recovery and limits

`Submitted`, received, acknowledged, reviewed, accepted and applied are different
states. A successful worker is still only **Ready for review**. No notification
accepts a patch, merges a branch or changes user authorization.

An uncertain external submission becomes `Unknown` and is never retried
automatically. Review the actual chat and runtime evidence before an explicit
trusted retry. Restarting a host without notification flags leaves notifications
off even if its previous manifest recorded an enabled policy; the managed
approved-task notice is attached only during a truly eligible initial fresh
launch and does not restore itself on restart. Already-running hosts keep their
recorded policy, and stopped, failed, orphaned, unknown or non-fresh sessions are
never resurrected. Closing the GUI does not stop the separately owned host.

Two real native chats, busy-turn delivery, MCP refresh behavior and live
restart/recovery remain explicit acceptance gates. The local fixtures use fake
executables only; they do not send messages to the user's chats or call paid
provider APIs. macOS/Windows release support is also a separate gate.
