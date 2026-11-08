# Desktop Projects View

The desktop control panel presents a grouped **Projects** view as its primary observation surface (canonical navigation page `activity` with the `folder-open` icon), alongside **Tools**, **Profiles**, and **Settings**.

## Observable Grouped Data

`projectProjects` (`apps/desktop/src/pages/projects-model.ts`) and `ProjectsPage` (`apps/desktop/src/pages/projects.ts`) project read-only `ObservationSnapshot` state into a three-level hierarchy without mock sessions or fabricated usage metrics:

1. **Projects (`ProjectGroupView`)**: Grouped by observed workspace/context path or linked runtime source, tracking active running counts across child sessions.
2. **Sessions (`ProjectSessionView`)**: Grouped per conversation binding or thread within a project, summarizing session lifecycle state (`working`, `waiting`, `failed`, `completed`), running call counts, attention flags, inline jobs (`ProjectJobView`), provider/model calls (`ProjectCallView`), and collapsible recent lifecycle events (`ProjectEventView`).
3. **Calls and Inline Plans**: Each call displays observed role, provider brand, requested/configured model with observed model and effort evidence when available, elapsed duration, and observed token usage when recorded (unknown usage stays unknown; no cost field exists or is displayed). Observed job plans and task summaries render inline within the session body.

## 6h / 24h Window Filter and Loaded-History Warnings

- **Window filter (`ProjectsWindowHours = 6 | 24`)**: Filters completed/inactive sessions to the selected `6h` or `24h` recency window relative to the snapshot timestamp. Sessions with running work or requiring operator attention remain visible regardless of age. When an exact job scope (`ActivityFeedScope`) is active, the scoped target remains visible.
- **Loaded-history boundary**: Filtering operates strictly on the currently loaded observation pages. When older durable history pages remain unloaded (`hasMore` or `jobsHasMore`), when sessions are filtered out by the time window (`hiddenSessionCount`), or when a linked source cannot be read (`unavailableSourceCount`), the view surfaces explicit notes and warnings rather than implying a complete global scan.

## Stable Ordering and Quiet Refresh

- Project groups, sessions, jobs, calls, and recent event rows use deterministic ordering keyed by durable identifiers and timestamps.
- Background polls that only advance `generated_at_ms` update relative time labels in place (`refreshTimeLabels()`) without replacing DOM nodes or collapsing user-expanded `<details>` disclosures.

## Scoped Job Focus, Inline Plans, and Approval Boundaries

- Navigating to a specific job (`openActivityForJob` / `openPlansForJob`) opens the primary **Projects** view with an exact `(source_id, job_id)` observation scope and focuses/expands the matching job (`focusJob`), with an inline action to clear the scope (`showAllActivity`).
- Legacy runtime history and the advanced plan ledger remain accessible only under **Settings → Advanced diagnostics** (`Legacy activity diagnostics` and `Advanced plan editor`).
- **Codex approval and execution invariants**: The Projects view is strictly read-only. Native Codex remains the conversational interface and approval authority; the desktop UI never starts, steers, or authorizes paid coding workers.

## Current Verification Limits

- Live acceptance remains scoped to the single-chat Linux/NixOS desktop workflow.
- Multi-chat concurrent orchestration and macOS/Windows desktop execution are not claimed as validated.
