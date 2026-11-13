# Minimal chat MVP acceptance — 2026-11-13

**Status: the human-approved minimal MVP passed its Linux/NixOS acceptance
gates on 2026-11-13.** The frozen runtime source is `3686e8deb975e22199ea479044dd645d91e2cd91`.
This report does not claim a host system upgrade or support for untested platforms.

## Accepted product boundary

Native Codex remains the conversational Coworker. Projects opens a single
chronological, bottom-growing chat event stream and a small read-only backlog
drawer with Todo, Doing, Done and Blocked groups. Active/Archive, the collapsible
sidebar, Tools, Profiles and Settings remain available. Ordinary chats have no
Jobs/Messages/Activity tabs, old age filters, or load-older controls.

An approved Worker still freezes its profile, registry revision, executable,
model, effort, repository, base and write scope. The ordinary path does not
require an Orchestrator, Reviewer, Closure, job or controller. Independent tasks
may run concurrently in isolated worktrees. Backlog state never grants authority.

Working requires fresh native activity or verified physical Worker presence.
Doing alone does not imply liveness. Needs you requires a fresh actual native
human request; a failed Worker, Blocked item, historical job or successful result
awaiting a Coworker check does not manufacture human attention.

The host persists the terminal result before notifying Codex. The Coworker reads
the original scoped result, independently checks physical delivery and its
acceptance condition, and records durable Done or a reasoned rejection. Rejection
preserves immutable technical success and moves the item to Blocked. A delivery
receipt, ACK, exit zero or agent prose alone is not acceptance. Coworker-only
completion is explicitly an attestation rather than evidence of Worker execution.

## Validation evidence

| Gate | Evidence and scope |
| --- | --- |
| Combined Rust workspace | 1,325 passing tests; 2 ignored tests (including the explicitly invoked real-presence collector). Formatting and all-target Clippy passed using the combined checkout's own target directory. |
| Guarded acceptance/recovery | Eleven focused guarded native tests and 166 native library regressions: stale revisions, cross-chat/authority mismatch, duplicate start, actual serialized reply loss and reconnect, one fake-provider invocation, immutable Done/rejection, changed physical delivery, missed notice and stopped-host result recovery. New actual serialized RESULT/DONE reply discards survive fresh MCP epochs with exact digests/history and one invocation; a genuine host dispatch before backlog receipt attachment resumes the same execution without another dispatch. |
| Desktop and host | 188 desktop tests passed at the integrated UI checkpoint; one pre-existing manual Codex-navigation test excluded. Host regressions passed, including noncreating read-only presence, bounded acquisition, deadlines and cancellation. Explicit opt-in real snapshot collectors were separately invoked against authorized live cases. |
| Frontend | 36 test files, type checking and production Vite build passed. |
| Production browser components | **36/36 passed**, with no skips or retries. Actual production entry point; explicitly test-only Tauri IPC. Covers scope isolation, expiry, false attention, committed completion, Active/Archive, Tools/Profiles/Settings, keyboard/touch navigation, drawer contrast and reader scroll through resize/reflow/head eviction. |
| Real native idle return | Two independent real DeepSeek Flash 4.1 Max Workers returned to separate actual native Codex chats. The actual Sol High Coworker inspected physical delivery, recorded Done and ACKed the real batch. |
| Real native busy return | Worker callback was handled inside the same already-active native turn. Physical delivery, real ACK and durable Done were independently verified. |
| Real physical liveness | The production read-only host API and actual packaged desktop IPC captured the same owned running Worker, exact scoped identities and original sample/expiry timestamps. The actual GUI rendered the Worker row and Working spinner. After native checked Done it showed no live Worker, Working or Needs you; a separate read-only reopen of stopped state preserved Done. |
| Fresh chat before work | A newly paired actual native chat appeared in the final packaged desktop before any backlog item, approval or Worker existed. The same real Tauri IPC and GUI then showed actual physical Worker Working and later checked durable Done with cleared Working/Needs you. A separate captured-JSON browser render remains explicitly mocked component evidence, not the basis of this continuous live claim. |
| Independent parallel execution | Two real Workers on one runtime artifact overlapped for 16,283 ms, with separate chat/binding/approval/item/execution/run/worktree identities. Both reached checked durable Done. |
| Nix artifacts | Both packages built offline from `3686e8d`. All four final CLI/MCP/host/session ELF files are byte-identical to the live-qualified runtime; the desktop includes the final navigation fix. Immutable artifact paths are listed below. |
| Isolated native NixOS application | **15/15 passed** on the final packaged candidate, with owned scene cleanup. Real packaged Tauri/WebKit app over a guest-only genuine Core fixture and original native IPC; no guest Codex backend or provider. Four backlog states and eight ordered events, actual wide/narrow drawers and readable titles. Separate real Wayland/StatusNotifier tray checks cover geometry, monitor/scale, pointer transit, click-outside/Escape, reopen, hide/open and Quit. |
| Review | One authorized Astra High package-closing review; targeted rechecks after the identified scroll/sidebar fixes and final response-loss tests. The narrow navigation portal opacity/contrast correction also passed a targeted source recheck. |

Private receipts, raw model logs, runtime identities, context files and captures
remain outside source control. The report summarizes their results without
publishing private conversations or credentials.

## Failures found and corrected

- Several initial browser failures came from malformed fixtures: repeated item
  revisions, invalid sample TTLs, and genuine native waiting state accidentally
  inherited by historical-job negative cases. Fixtures now preserve actual
  production invariants; assertions were not weakened to conceal product bugs.
- Actual responsive reader-scroll drift was caused by programmatic scroll events
  replacing the user's stored first-visible anchor during two-phase layout. A
  scoped self-scroll guard now preserves that anchor. Strong event-ID/pixel-offset
  checks passed across narrow reflow and resize; deterministic head eviction is
  tested separately.
- Wide age/action labels left chat titles unreadable. Unknown timestamps are
  omitted, known ages moved below the title, titles can use two lines, and compact
  Archive/Restore controls retain full accessible names and keyboard/touch access.
- Manual inspection of the packaged native screenshot found a transparent narrow
  navigation portal despite passing geometry/behavior checks. The portal now
  receives its own opaque theme tokens. The new browser regression failed with
  alpha zero before the fix, then passed in light and dark themes. The final
  packaged native drawer also passed actual opacity and contrast assertions, and
  its rendered screenshot was inspected.
- The first live-presence collector sampled before a physical process was visible.
  That observation case remains failed; its original Worker was reconciled to
  checked Done without redispatch. A bounded read-only collector wait qualified
  the later busy case. No unknown effect was blindly replayed.
- One sidebar implementation call timed out after producing its patch. The parent
  preserved and reviewed the patch, verified the pinned observed model, and ran
  the actual acceptance checks; the timeout is not reported as Worker success.

## Limits

Qualified environment: Linux/NixOS, native Codex managed local route, the selected
DeepSeek Flash 4.1 Max Worker and Sol High Coworker used in the live cases.
Fake-provider recovery tests and guest-only native UI fixtures are identified
separately from real-provider/native-chat evidence. No claim covers every
provider, model, compositor, prolonged app-closed delivery, or interactive Worker
Steer capability. Core recovery evidence covers the tested disconnect/reopen and
stopped-host cases, not unlimited outage tolerance.

The native UI/tray package was tested in an independent NixOS/Wayland guest.
Continuous real-chat GUI acceptance separately used an owned headless Wayland
scene on the existing host, with private HOME/XDG/D-Bus/display and the existing
verified Codex backend. Neither route used host activation, user Codex restart,
a second backend, global configuration changes, or modifications to /etc/nixos. This report does not claim an installed-host
upgrade or migration acceptance. macOS remains unqualified; Windows is deferred.

## Recovery fault boundary coverage

| Boundary | Controlled evidence |
| --- | --- |
| Reservation / pre-effect admission | Host-not-ready refusal preserves the original durable reservation and zero provider invocations; exact authorized resume invokes once. Invalid/stale/cross-chat authority leaves state unchanged. |
| Uncertain admission / dispatch intent | Durable unresolved intent refuses redispatch rather than fabricating success or replacing the invocation. |
| Spawn before backlog receipt | A real fake-provider host invocation completes while the original backlog reservation has no dispatch receipt; exact facade resume attaches that genuine run/worker receipt with invocation count remaining one. |
| Dispatch receipt / lost START reply | Actual serialized JSON-RPC reply is deliberately discarded, transport closed and a fresh enrolled MCP epoch recovers the same execution without another spawn. |
| Terminal persistence / lost notice | No notice is consumed; host stops, authority reopens, original terminal result and physical delivery are recovered before Done. |
| Result reconciliation / lost RESULT reply | Actual serialized result reply is discarded; fresh epoch recovers one persisted outcome/history entry and original digests. |
| Done commit / lost DONE reply | Actual serialized completion reply is discarded; a fresh epoch recovers Done. Exact original command replays the same receipt even after checkout removal, with no extra revision/history/provider call. |

These are controlled transport/state interruptions, not physical process-kill or power-loss injection at every machine instruction. Raw legacy approved-task status may remain ready_for_review while the guarded backlog is Done; that legacy field is not the ordinary workflow completion or human-attention source.

## Frozen artifacts

- Runtime source: `3686e8deb975e22199ea479044dd645d91e2cd91`.
- CLI/MCP package: `/nix/store/dkpgfq47ba9bm8i3iv9ihfj63gj1wdnb-threadlane-0.1.0`.
- Desktop package: `/nix/store/g9nvirs5ln5gkwslwn9fi7rq3bfwxdmy-threadlane-desktop-0.1.0`.
- Native navigation background alpha: `1`; measured title contrast: `12.34:1`.

The regression tests, report and status documents may be committed after this runtime checkpoint.
A test/report-only descendant does not change these tested executable bytes.
