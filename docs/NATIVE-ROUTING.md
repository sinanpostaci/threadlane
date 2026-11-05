# Native chat routing gate

Historical routing review: 2026-10-29. The fixed-binding description below
records that checkpoint. The current managed Linux route starts unbound and uses
verified `connect_current_chat` enrollment; see [native pairing](NATIVE-GATE.md)
and [the operating guide](MCP-OPERATING-GUIDE.md). General multi-chat and other
platform acceptance remain separate gates.

Codex host MCP configuration is shared by desktop/CLI/IDE clients; trusted
project configuration can override it. A project setting does not by itself
establish which chat made a tool call. See the official [MCP configuration](https://learn.chatgpt.com/docs/extend/mcp).

App-server exposes thread-scoped MCP connection lookup and tool calls. The
reviewed documentation does not establish an authenticated caller chat ID passed
to an arbitrary third-party stdio MCP server. Absence from these docs is not proof
that no compatible mechanism exists. See the official [app-server protocol](https://learn.chatgpt.com/docs/app-server).

At that checkpoint, Threadlane fixed `binding_id` in the trusted MCP launch command and
checked binding scope in its HTTP/core APIs. This isolates an instance's admitted
scope; it does not authenticate the identity of whichever native chat received
that same server configuration. The observed native experiment used one
explicitly authorized disposable chat. Do not install a single fixed binding as
universal routing for every chat in a shared project.

Before automatic per-chat onboarding, demonstrate two chats in one project with
independent binding authority. Prove that each call/event/wake reaches its own
scope, wrong-scope actions fail, restart/refresh preserves the association, and
busy-turn routing does not alter user model, permissions or conversation state.
Caller prose, titles, cwd and a model-supplied UUID are not identity proof.
Prefer documented trusted client context or an explicit verified adapter. Do not
inspect private application databases or invent undocumented IPC to fill the gap.

The Milestone 2B worker dispatch fixture remains a single configured binding.
A passing HTTP/stdio fixture must not be reported as general native multi-chat
acceptance.


## Milestone 2C single-chat experiment

The primary verified a real native worker edit/return loop and a separate
process-only cancellation loop in one explicitly authorized disposable chat.
The initial already-loaded chat retained its older MCP connection after project
configuration changed; terminating that stale test child yielded `Transport closed`
rather than automatic refresh. This observation is not a universal client rule.

A fresh same-directory fork, created through the public desktop tools, loaded
an owned test-only stdio launcher. Before MCP initialization completed, the
operator sealed that new chat's known binding in a private file and the launcher
executed the scoped Rust MCP server. Actual native tool records and the runtime
ledger were compared. No application-private database, guessed IPC protocol,
model/security override or app-wide restart was used.

That launcher is a disposable probe, not production onboarding or authenticated
per-call identity. Another chat sharing the same configured server could receive
the same admitted scope. The same-project two-chat authority gate above therefore
remains unresolved. All owned temporary trust/MCP entries and test processes were
removed; bounded private fixture evidence and dirty task worktrees were preserved.
