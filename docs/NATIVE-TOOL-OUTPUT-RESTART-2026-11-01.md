# Installed desktop bridge: post-restart check (2026-11-01)

This is a qualified follow-up to the [controlled product pilot](NATIVE-TOOL-OUTPUT-PILOT-2026-10-31.md).
It covers one existing disposable native chat on Linux/NixOS. It does not
establish automatic restart recovery or general multi-chat routing.

## Installation and endpoint

The human activated the prepared NixOS integration and reported a restart.
The installed user launcher resolved to the same `26.1027.30824` bridge package
used in the earlier controlled experiments. Process checks verified one native
application, its translator, and exactly one original bundled backend.

The newly created endpoint was selected through that application's private
descriptor and checked against the actual process ancestry, executable paths,
process start identities and same-user socket permissions. The old socket was
not reused. No additional native backend was started and no application was
closed by the probe.

The operator opened the same authorized test chat through the public app tool.
The public metadata reported its original working directory. A metadata-only
handshake and resume with excluded turns confirmed its existing Luna / `max`
configuration before any model turn; no model, effort or permission override
was supplied.

## Transport receipt and visible response

The initial passive probe reached the idle chat as `functionCallOutput` and
completed, but its final assistant text was empty while the previous pilot's
no-further-action teardown instruction remained in effect. This was recorded
as transport receipt, not a visible-response pass.

After separate explicit human authorization, one visible operator setup message
limited the next probe to acknowledging its exact identifier without tools,
files, approval or dispatch. The setup completed with
`READY_FOR_RESTART_PROBE`. The subsequent distinct probe then:

- Revalidated the same application processes, chat, directory and Luna Max.
- Sent one `turn/start` with empty user input and a named `threadlane_probe`
  tool output containing an independently generated identifier.
- Completed with the expected `functionCallOutput` and an assistant sentence
  acknowledging that exact identifier and stating no action was requested.
- Contained no user message in the machine-notification turn. The separately
  approved setup was an intentional visible operator message.

The public chat reader independently confirmed the completed turn and its text.
The human also confirmed seeing that response in the native application. No
worker was dispatched and no provider other than the existing Luna was called.

## Existing project control process

The existing Threadlane project context reported its control supervisor offline
after restart, including outside the restricted diagnostic sandbox. The operator
reopened only that single-owner supervisor with the existing runtime and binding
in an owned tmux session, with worker configuration and notifications omitted.
The existing context then passed its live binding check. No project context,
profile, model assignment or persistent OS service was changed.

This control-plane check is separate from the disposable chat's native delivery
test. A verified local binding remains `native_per_call_identity_verified: false`
and grants no human approval or worker-dispatch authority.

## Remaining boundaries

Endpoint refresh was explicitly selected and verified by the operator. There is
still no automatic adoption of a new socket, replay of uncertain notices,
app-closed wake-up, verified SIGKILL/orphan recovery, or macOS/Windows acceptance.
The application must be loaded and the selected chat's settings must match the
explicit notification context. The follower also needs an applicable authorized
handling instruction; transport receipt alone does not guarantee a user-facing
summary or event acknowledgement.
