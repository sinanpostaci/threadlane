# M4 local workflow checkpoint

This is a Linux pilot with local fixture coverage. Native Codex remains the user's
conversational interface. Threadlane does not listen to conversation transcripts,
interpret worker prose as operator approval, or silently choose another model.

## Implemented boundaries

- Editable global tools, installed CLI discovery, model/effort catalogs, API
  credential controls, workflow profiles, defaults, and four difficulty levels.
- Durable plan drafts and explicit publication over the desktop or paired MCP.
  Publication does not start a worker or grant execution authority.
- Operator-only immutable approvals that freeze the published revision, registry
  version, repository, exact Git base, tasks, models, efforts, instructions, write
  paths, concurrency, and execution budgets.
- One separately owned host per approval. The desktop can explicitly start or
  attach to it, but closing the GUI does not stop it. Readiness requires an actual
  authenticated supervisor request, not just a PID or a manifest.
- Paired MCP exposes 27 tools, including six approval/session operations. Every
  call verifies the enrolled native connection epoch and original control
  binding. Approved dispatch accepts approval/task IDs rather than caller-supplied
  instructions, models, paths, or command keys.
- Independent root AGY/OpenCode tasks run in owned worktrees. Dependent tasks stay
  blocked until predecessor inputs can be verified; Codex roles remain native
  conversation routes rather than headless CLI workers.
- A dispatch intent is durably recorded before the runtime effect. Missing run
  mappings remain `Unknown`; reading status cannot start a worker. Explicit
  recovery reuses the frozen idempotency key. Completion remains **Ready for
  review**, with `accepted: false`.
- Exact-file write scopes never become recursive AGY grants. Directory scopes
  are explicit, and actual task launches use the manifest's resolved Git commit.
  Post-run diff checks remain separate from launch permissions and review.
- Read-only scoped history and per-approval session/task observation. The session
  panel preserves control-binding identity, rejects stale responses, and stops
  issuing reads while hidden or detached.
- English UI and documentation, system appearance preferences, provider icons,
  theme/accent/transparency controls, and locked Nix CLI/desktop packages.

## Validation and review

The combined execution corrections passed 215 unique Rust tests in a full
workspace run, all-target Clippy with warnings denied, and formatting. The final
manifest-pinning change received separate worker/runtime checks, including the
symbolic-ref control. That control is deterministic component evidence, not a
reproduction of a live concurrent Git-ref race.

The standalone Tauri backend passed 13 helper tests and all-target Clippy. An
authenticated startup fixture also passed three repeated runs after correcting
atomic temporary-file handling. The frontend passed production typecheck/build
and 45 unique tests. Python tooling passed all 23 tests. Actual MCP stdio
discovery returned 27 tools, and all six approved operations rejected unbound
connections.

One Astra Max package review identified two P2 issues: overly broad exact-file
AGY grants and an unsealed dispatch being reported as not started. Both production
fixes passed its targeted recheck. No second full package review was performed.

Nix source builds verify both CLI and Linux desktop packaging, including the
matching host companion on the desktop process PATH. Nix package builds have
`doCheck = false`; their success does not stand in for the separate test runs.
Dependency versions were preserved while adding local workspace dependency edges.

Browser acceptance at 390 pixels covered draft save/publication, read-only sample
approval, no automatic execution, preserved editor focus/caret during polling,
and no horizontal overflow. These checks use explicit DEV fixtures and do not
establish native conversation identity or live provider access.

## Live acceptance still required

- Two distinct native Codex chats with verified per-call pairing and routing.
- Native MCP restart/refresh and busy-turn notification delivery.
- Automatic notification from the separately owned approval host into the native
  follower conversation. Approved host activity is currently observed through
  the Plans panel and MCP reads; the older runtime wake-up adapter is a separate
  path and is not enabled on these hosts.
- Current native desktop visual/interaction acceptance of the complete workflow.
  The earlier native Wayland rendering confirmation belongs to the prior preview.
- User-entered product API credentials and explicitly initiated real API tests.
- macOS/Windows packaging and runtime acceptance. Windows lifecycle/private-file
  support remains explicitly unsupported; the manual CI matrix was not run.

Automatic DAG scheduling, native-role execution, merge/push, and cleanup do not
follow from a worker exiting successfully. Those operations need their own
verified inputs and trusted decisions.

No new live chats, paid product API tests, persistent OS services, global
configuration changes, NixOS edits, or system activation were performed for this
checkpoint. The current setup guide is [Configuration](CONFIGURATION.md).
