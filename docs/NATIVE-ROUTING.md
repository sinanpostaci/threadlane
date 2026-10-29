# Native chat routing gate

Documentation review: 2026-10-29. This is a later integration gate, not a claim
that general multi-chat routing is implemented.

Codex host MCP configuration is shared by desktop/CLI/IDE clients; trusted
project configuration can override it. A project setting does not by itself
establish which chat made a tool call. See the official [MCP configuration](https://learn.chatgpt.com/docs/extend/mcp).

App-server exposes thread-scoped MCP connection lookup and tool calls. The
reviewed documentation does not establish an authenticated caller chat ID passed
to an arbitrary third-party stdio MCP server. Absence from these docs is not proof
that no compatible mechanism exists. See the official [app-server protocol](https://learn.chatgpt.com/docs/app-server).

Threadlane currently fixes `binding_id` in the trusted MCP launch command and
checks binding scope in its HTTP/core APIs. This isolates an instance's admitted
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
