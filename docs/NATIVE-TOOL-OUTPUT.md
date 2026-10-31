# Native tool-output notifications

This explicit operator transport delivers the existing approved-session wake-up
notice as a Codex `functionCallOutput`. It uses the documented app-server
WebSocket protocol over a selected local Unix socket. The native Codex chat
remains the conversational interface.

The host notification adapter never starts another Codex server, injects user
text, switches models, or falls back to the legacy queue when this transport is
selected. Notifications remain off by default. The existing paired MCP and
trusted operator CLI are separate routes for pulling and acknowledging events.

## Operator configuration

Create an explicit private JSON context file in a directory owned by the current
user with mode `0700`. The file must be a regular, non-symlink file with mode
`0600`; its maximum size is 16 KiB. Use canonical physical paths, including a
currently existing Unix socket in its own private directory.

```json
{
  "schema_version": 1,
  "socket_path": "/private/runtime/native-codex/control.sock",
  "thread_id": "01900000-0000-7000-8000-000000000001",
  "project_root": "/canonical/path/to/repository",
  "model": "gpt-6-luna",
  "effort": "max"
}
```

The model and effort are expected existing settings, not overrides. The
notification fails before inference when the native chat reports different
settings. The selected thread must already be loaded. The context is an
operator-selected local scope; it does not authenticate native MCP per-call
conversation provenance or prove automatic multi-chat isolation.

Start an existing approved session with the normal store and approval arguments,
plus the following explicit notification options:

```text
--notify-codex /absolute/path/to/trusted/codex
--notify-tool-output-context /private/path/native-notification.json
--notify-route operator-cli
--notify-timeout-ms 10000
```

The executable remains part of the existing validated policy. The tool-output
transport does not execute it. The `mcp` and `operator-cli` route options select
fixed software-owned pull/ACK guidance; they do not change the transport.
Restarting a host requires an explicit opt-in again. Old manifests cannot
silently restore a socket, model, or notification policy.

## Scope and delivery

Before starting the host, the context's thread and canonical project root must
match the stored approval. Before each notification, the adapter validates the
socket and checks native metadata, including the exact thread, working directory,
model and effort. It sends one `turn/start` with empty `input` and a named
`toolOutput` containing fixed metadata and the existing pull/ACK notice.

The durable supervisor claim owns event coalescing. Raw worker text, task
instructions and answers are not inserted into the wake-up notice. Events are
pulled through the existing scoped tools and handled as untrusted data. A notice
cannot approve, dispatch, review, integrate, merge or publish work.

A valid returned turn ID establishes **Submitted**, not visible delivery,
processing, acknowledgement, or patch acceptance. The follower must handle the
actual returned event batch and acknowledge that batch's actual `batch_id`.
The observer UI does not consume or acknowledge events.

## Failure and recovery

Connection, scope and configuration failures before the effect-bearing write
record **Failed**. Once writing `turn/start` may have begun, disconnects,
timeouts, invalid responses and cancellation record **Unknown**. There is no
automatic resend, fallback, model interruption, or replay after an uncertain
outcome. Inspect and reconcile the actual chat and durable claim first.

An app restart can invalidate the selected socket. Refresh the explicitly
selected context and host policy through the operator workflow; do not scan for
and adopt a different server or infer identity from a chat title. A descriptor
alone is not per-chat routing proof.

## Evidence and platform limits

On 2026-10-31, a separately authorized temporary Linux/NixOS desktop package
exposed the original UI-owned Codex backend through a private Unix WebSocket.
The original Codex backend and UI bundle bytes were preserved. There was exactly
one backend. The human confirmed two visible results in the same disposable
native chat, using its existing Luna Max configuration:

- An idle chat with no pending tool call received a standalone tool output and
  completed a reply without a new user message.
- During a separately authorized test setup's sleep operation, a second tool
  output was accepted into the same active turn. The reply acknowledged the
  independently generated event ID. Only the intentional setup was a user
  message; the machine event remained `functionCallOutput`.

These controlled protocol experiments do not by themselves establish the new
product host's full end-to-end delivery, app-closed wake-up, restart persistence,
general multi-chat routing, or platform release support. Tests with fake local
WebSocket servers are transport and lifecycle evidence, not live user delivery.

The native desktop launcher used by that experiment is still a private test
package. This change does not install it, alter the user's NixOS configuration,
change global MCP settings, or install a background service. macOS needs its own
native launch, identity, application signature and update validation; Windows
needs a separately implemented local transport. A Tauri build alone does not
verify native conversation delivery on either platform.

## References

- [Codex App Server](https://learn.chatgpt.com/docs/app-server)
- [Existing approved-session notification policy](APPROVED-NOTIFICATIONS.md)
- [Trusted operator CLI pull and ACK route](NATIVE-OPERATOR-NOTIFICATIONS.md)
- [Native per-call routing gate](NATIVE-ROUTING.md)
