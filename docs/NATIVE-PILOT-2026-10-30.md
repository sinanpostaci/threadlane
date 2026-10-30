# Native Codex single-chat qualification (2026-10-30)

The installed Codex 0.165.0-alpha.1.2 / desktop 26.1023.51421 was exercised in an explicitly authorized temporary project. Product source was unchanged.

- Project-only MCP discovery, unbound refusal, fixed software wake-up, native event pull/ACK, question return, revision replacement, stale-answer rejection, current answer, completion and supervisor restart passed with a simulated worker. Final active runs were zero, sequence nine was acknowledged and wake-up state was idle. Revoking the isolated epoch produced `expired_epoch`.
- `codex queue` delivered a distinct exact acknowledgement in an existing chat. When the chat was busy, the queued message ran in a later turn after the active turn. This does not establish programmatic mid-turn steer. Submission exit status was checked against actual native replies.
- The default public app-server control socket was absent. No private Codex database or undocumented IPC was used, and no second app-server was started. The fixture used explicit operator association of one known native MCP child. Native per-call identity remained `needs_verification`; automatic onboarding and same-project two-chat isolation were not established.
- This was the paired MCP/core supervisor demo, not live approved-host notification acceptance or a coding-provider task. Native MCP reconnect, macOS and Windows remain untested.
- Temporary project trust and MCP configuration were removed, global configuration hashes matched their originals, owned test processes stopped, the fixture association was revoked and the new test chat archived. No persistent service or user model changes were made.
