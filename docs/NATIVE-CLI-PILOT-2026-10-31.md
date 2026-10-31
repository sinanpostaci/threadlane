# Native operator CLI pilot (2026-10-31)

This checkpoint records one explicitly authorized Linux/NixOS pilot in a single
trusted local operator CLI conversation. It is transport and runtime evidence for
that scope, not a general native onboarding, multi-chat, provider, or platform
claim. No worker-produced code was accepted as a result of the pilot.

Validated source: `623959fae2a656c8b50fffb02b50d303e6b5257d`.

## Scope

- Native Codex remained the conversation surface. Threadlane was reached through
  the trusted local operator CLI rather than a desktop form or a newly enrolled
  MCP chat.
- The pilot used one private context and one conversation. General per-call native
  identity, automatic same-project two-chat routing, and multi-chat isolation were
  not exercised.
- The older restricted test chat was not used for CLI execution or bypassed. Its refusal was
  respected, and no attempt was made to weaken that restriction.

## Observed results

- A real callback was observed after a busy native turn. The follower then pulled
  a bounded six-event batch and acknowledged it using the actual returned batch
  identifier.
- A real OpenCode worker process was canceled. Cancellation emitted a
  `system_init` step-start record. That record is not proof of model output,
  requested-versus-observed effort telemetry, or termination of remote inference.
- Explicit idle stop, explicit restart, and no automatic replay of the stopped
  session passed. Receipts were preserved.
- No worker result was accepted; this cancellation pilot invoked no review acceptance,
  merge, push, or main publication.
- The Nix CLI and desktop builds passed. Native window creation was observed for
  the latest desktop build; the visual human check of that window is still pending.

A focused 142-test checkpoint covered the targeted components exercised around
this work. It is historical evidence, not a full workspace suite, a platform
matrix, or a substitute for reproducible validation and review. Earlier test
totals and dated checkpoints remain historical claims for their own scope.

## Configuration and cleanup

- Global MCP and Codex configuration were left untouched.
- No OS service or autostart registration was installed. The explicitly launched
  local control process is separate from an OS service.
- The pilot used private runtime stores and stopped only its owned processes.
  This report excludes raw conversation, provider logs, account data, and private
  paths; those remain outside the repository.
- Packaged operator-context guidance is maintained separately in
  [Native context commands](NATIVE-CONTEXT.md); this pilot does not redefine it.

## Earlier, separate code-generation checkpoint

On product source `0e94e170be497885e88d14f4559cefa1b32a3bf9`, a disposable
three-task OpenCode Go pipeline completed two independent functions and one
reviewed dependent composition. All three tasks reported the requested
`opencode-go/deepseek-v4.1-flash` model and `max` effort through owned-session,
per-assistant CLI metadata. Explicit review and same-run integration receipts
were recorded; eight focused integration checks passed and the source checkout
remained unchanged. Its 48 acknowledged events and two automatic wake-ups belong
to that separately configured native MCP pilot, not the CLI cancellation test
above. CLI-reported evidence is not cloud attestation or general native identity.

## Remaining gates

- General native per-call identity and authenticated two-chat routing.
- Live interactive provider question/Steer and broader provider compatibility.
- Exact model-effort telemetry, and proof of remote inference termination where a
  provider does not expose it.
- Product API entitlement and live credentials.
- macOS and Windows packaging, lifecycle, and runtime acceptance.
- Completed visual acceptance of the current desktop window.

See [native operator commands](NATIVE-OPERATOR.md),
[approved-session notifications](APPROVED-NOTIFICATIONS.md),
[OpenCode metadata](OPENCODE-METADATA.md), and [validation](VALIDATION.md).
