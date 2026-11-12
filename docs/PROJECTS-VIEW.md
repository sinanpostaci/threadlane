# Desktop Projects View

The desktop control panel mounts **Projects** as its primary chat-first workspace
surface (`apps/desktop/index.html` → `apps/desktop/src/main.ts` →
`apps/desktop/src/app.tsx`), alongside **Tools**, **Profiles**, and **Settings**,
while preserving the `?surface=tray` route for the compact tray popover.

## Navigation: `Projects / Active` and `Projects / Archive`

Under **Projects** in both the wide desktop sidebar (`> 760px`) and the narrow
modal navigation drawer (`<= 760px`, Radix `Dialog`), two explicit lifecycle
submenus partition conversations without mutating backend state:

1. **Active (`Projects/Active`)**: Default view showing active project groups
   (`workspace.activeProjects`) and their chat rows, ordered by active human
   blockers, live Coworker/Worker execution, and recency.
2. **Archive (`Projects/Archive`)**: Separate view showing locally archived
   project groups (`workspace.archivedProjects`).

Each chat row provides a per-chat **Archive** (when in `Active`) or **Restore**
(when in `Archive`) control keyed by canonical four-part `WorkspaceChatIdentity`
(`source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`) and
persisted in `localStorage` under `threadlane.desktop.workspace.chatArchive.v1`.
Storage failures surface as an explicit alert banner rather than silently
discarding or faking persistence. If an archived conversation still has an active
human blocker, `Projects/Active` displays an explicit notice with a direct action
to open `Projects/Archive`.

Legacy `6h` / `24h` time-window chips, `Older (6h+)` pills, `Load older activity`
buttons, bulk `Archive history`, and `Show archived` cutoff controls are removed
from the production `Projects` flow.

## Titlebar-Only Sidebar Control and Responsive Layout

- The sidebar toggle button lives exclusively in the custom desktop titlebar.
- On wide viewports (`> 760px`), clicking the titlebar button collapses or
  expands the left `<aside>` rail in place.
- On compact viewports (`<= 760px` down to `360px`), clicking the titlebar button
  opens an accessible Radix `Dialog` modal navigation drawer supporting keyboard
  navigation, `Escape` dismissal, and focus restoration to the titlebar trigger.
- Top-level section panels (`Projects`, `Tools`, `Profiles`, `Settings`) remain
  mounted across section and viewport transitions so dirty `Profiles` drafts and
  per-chat scroll positions survive navigation.

## Selected Chat Tabs: `Messages` (Default), `Jobs`, and `Activity`

`DesktopAppCoordinator` fetches one bounded `WorkspaceSnapshot` from
`desktop_workspace_snapshot` (`ipcWorkspaceSnapshot`) and projects
`WorkspaceProductionProjection` via `projectWorkspaceSnapshot`
(`apps/desktop/src/app-model.ts`). `app.tsx` resolves the selected chat's raw
`WorkspaceChatSummary` and `WorkspaceNativeSample` from
`state.workspaceSnapshot` strictly by full four-part canonical chat identity
equality—never passing the whole snapshot as a chat, matching by `source_id`
alone, or spilling records across chats—and renders `WorkspaceChatPanels`
(`apps/desktop/src/workspace-panels.tsx`):

- **Messages (default)**: Compact, neutral, read-only list of safe recorded
  summaries, questions, answers, review verdicts, and job outcomes with distinct
  category icons. The message viewport follows new items at the bottom by default,
  switches to anchor-preserving reading mode when scrolled up, and surfaces a
  floating jump-to-latest button (`New messages — Jump to latest`) when new
  items arrive off-screen. No raw conversation transcript is read and no message
  composer is rendered.
- **Jobs**: Separate durable ledger of jobs, tasks, attempts, candidates,
  attention states, historical failures, and backend-verified acceptance proofs,
  with cross-tab navigation to inspect job-scoped `Activity`.
- **Activity**: Separate chronological audit feed of requests, child calls, and
  delivery stages with centered `ActivitySpinner` indicators on live calls and
  cross-tab links back to `Jobs`.

## Truthfulness, Bounded Omissions, and Authority Invariants

- **Bounded data visibility**: Production loads one bounded snapshot without
  multi-page history loading. Whenever messages, job histories, child calls,
  requests, or native samples are omitted by snapshot bounds—or when unscoped
  records are withheld—explicit notices keep the omission visible.
- **Independent Coworker state and model**: Live Coworker activity and verified
  configured model selection come from fresh native chat samples and remain
  separate from Worker runtime receipts. Stale or disconnected native samples
  withhold live activity/model claims.
- **No false completion or authority**: Completed local MCP calls and finished
  Worker processes never mark a conversation or job `Completed`. Native Codex
  remains the conversational interface and sole approval surface; `Open in Codex`
  uses `desktop_open_codex_chat` with backend chat-identity revalidation and no
  arbitrary URL or shell fallback.
- **Language preservation**: Fixed UI labels and diagnostics are English; user
  and agent chat titles, job titles, summaries, and messages are rendered
  verbatim in their source language.

## Current Verification Status

- Production entry wiring (`index.html` → `main.ts` → `app.tsx` →
  `WorkspaceChatPanels`) is implemented in source.
- Final combined-checkout Rust/frontend checks (after merging concurrent
  coordinator/IPC dependencies), wide and narrow Playwright browser acceptance,
  isolated NixOS Wayland VM tray and Codex routing acceptance, authorized live
  workflow qualification, and the single `gpt-6-astra` (`high`) package-closing
  review remain pending separate gates.
