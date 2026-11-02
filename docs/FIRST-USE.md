# First everyday use

Keep the conversation in Codex. Threadlane records approved work, runs the
selected workers in isolated worktrees and returns bounded events to that
conversation. The desktop is an optional observer and configuration surface.

## Check local setup

Open **Settings → Connections → Getting started**. This checklist reads the local registry and
runtime snapshot; it does not execute a worker or test paid model access.

- Configure an enabled CLI tool in **Tools**. System resolution follows the
  installed command; a custom executable path is an explicit override. Version
  inspection checks installed files, not upstream updates.
- Import the intended provider's models and efforts. OpenCode Go and API routes
  are distinct. Catalog visibility does not prove subscription entitlement.
- Save the desired role assignments in **Profiles**. A complete workflow has
  valid role defaults; difficulty overrides supplement those defaults. A saved
  model assignment does not authorize using that model.
- Attach the intended real runtime in **Settings → Connections**, then select
  its verified conversation binding in the same section.
  The local demo and browser preview do not establish a live Codex connection.
- Verify **Codex MCP startup registration** in **Settings → Connections**. On
  desktop startup, Threadlane registers or refreshes only its owned unbound
  `mcp_servers.threadlane` entry in `${CODEX_HOME:-$HOME/.codex}/config.toml`
  while preserving unrelated settings, comments, disabled entries, and manual
  edits. Restart or refresh Codex MCP connections when the entry changes;
  conversation pairing is still required before scoped MCP tools can run. See
  [Codex MCP startup setup](CODEX-SETUP.md).

The checklist cannot verify execution approval, notification delivery or an
account's remaining quota. Those are checked for the concrete session.

## Start with one small task

Use an existing private operator context for the intended repository and chat.
Keep paths, approvals and runtime logs outside Git. Do not copy another chat's
binding or use a chat title as identity.

```sh
threadlane-context --context /private/project-context.json workflow catalog
```

Ask Codex to prepare one bounded task, including the model, effort, write paths,
base commit, timeout and checks. Inspect the frozen execution preview in the
conversation and approve that exact scope. Existing operator commands then
record the approval, explicitly start the host and dispatch only approved tasks.
See [native operator commands](NATIVE-OPERATOR.md) and
[approval](NATIVE-APPROVAL.md).

For the first run, keep the change small enough to inspect independently, such
as one documentation improvement. Do not begin by automatically merging a
large generated patch. Completion means ready for review; it does not mean
reviewed, accepted, merged or pushed.

## Progress and changes of plan

Use the explicitly selected notification route for the approved session. A
notification carries software-owned identifiers and pull/ACK instructions,
never new execution authority. The follower pulls events, summarizes relevant
progress or questions and acknowledges the actual returned batch ID.

Continue asking questions or steering in Codex. Record scope and model changes
before advancing affected work. A changed scope needs a fresh frozen approval;
an old approval is not rewritten. Unsupported live input stays pending rather
than being reported as delivered. See [conversation changes](CONVERSATION-CHANGES.md).

## After an interruption

Use **Activity** for the chronological request feed and **Plans** for independent
jobs. Link an existing private project context under **Settings → Connections**
when its durable stores differ from the configured defaults. For the older
selected-runtime recovery view, open **Settings → Connections → Advanced controls
→ Legacy activity diagnostics → Needs attention** to inspect uncertain or unsuccessful recent runs. That diagnostic overview is bounded to the selected snapshot; it is not an inventory
of every approval or host. Use run details and the scoped operator commands for
the exact task, worktree, host instance and notification cursor.

Threadlane preserves durable state and marks interrupted in-flight work
`unknown`. It does not replay provider requests, grant a new approval, restore
host startup or resume an LLM at its last token automatically. Inspect existing
changes and reconcile uncertain effects before deciding what to run next. See
[recovery](RECOVERY.md) and [notification recovery](NOTIFICATION-RECOVERY.md).

## Acceptance limits

The initial supported pilot host is Linux/NixOS. Existing live tests verify the
selected private Codex bridge; they do not prove a wrapper-free shared backend,
general multi-chat identity, physical power-loss durability, or macOS/Windows
notification delivery. Saved setup and a successful transport submission do not
replace a reviewed real-task acceptance test.
