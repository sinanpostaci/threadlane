# Desktop observation

The desktop is an optional observer. Keep planning, questions, scope approvals,
work dispatch, and follow-up in Codex. Opening Activity or Plans does not start a
worker, select a model, approve a task, consume an event batch, or replay work.

## Activity: what Threadlane was asked to do

Activity is a chronological request feed across the configured observation
sources. The most recently updated requests appear first. Progress updates the
existing request and its causal calls instead of creating a new row for each
output fragment.

A collapsed row identifies the project and job, operation, known requester,
executor, model/effort, elapsed time, and reported usage. Open it to inspect child
calls and the return timeline. Provider icons identify the executing tool;
spinners are reserved for calls with current execution evidence. The requester
is separate from the executor. Configuration can identify an intended follower
without proving that model made a particular request.

`View job` opens the exact source/job pair in Plans. `View activity` in Plans
applies the inverse filter. Two jobs in the same chat, or identical IDs in
different sources, remain separate. Expanding a row or refreshing a page does
not change workflow state.

### Evidence labels

- **Requested / observed model and effort:** a profile or approval records the
  request; only a runtime/provider receipt establishes what actually executed.
- **Not reported:** the source did not report a value. It is not zero. Token
  totals are not inferred from duration or from another model's usage, and parent
  and child totals are not added together.
- **Cache hit / network response:** a Jev cache read is distinguished from a new
  provider request. Viewing the feed never makes a Jev request.
- **Unknown:** execution or delivery evidence is missing or interrupted. A saved
  running row or a reused PID is insufficient to prove a process remains live.
  Observation does not restart it.
- **Ready for review / accepted:** producing output is different from accepting
  the current candidate. Reviews require the matching task/candidate receipt.

### Result delivery is a timeline

The feed distinguishes result ready, notification submitted, batch delivered,
and batch acknowledged. A synchronous API/CLI return is separately recorded as
response returned. Notification submission does not prove a chat read it. An ACK
proves acknowledgement of the recorded batch; it does not prove a human saw the
result or that a particular model explained it. A reviewer status does not
itself dispatch a reviewer.

## Plans: independent work, with less machinery in the foreground

Plans groups independent jobs by project. A job shows its intended outcome,
current lifecycle, steps and dependencies, approval/execution/review state, and
recorded activity. Several jobs can be active in one project or chat.

Technical identifiers and revisions are behind disclosure controls. Advanced
plan controls preserve the existing explicit approval and execution guards;
opening the simple overview is read-only. Partial, failed, and stopped work
remain inspectable when their durable records are available.

## Settings and source connections

Navigation is Activity, Plans, Tools, Profiles, and Settings. The previous Chats
linking and setup controls are under **Settings → Connections**. Tools owns
executable paths, credentials, and model catalogs. Profiles owns role/model/effort
assignments and the optional Decision advisor. Settings has no separate synthetic
Decision test panel or duplicate advisor selection.

The feed reads the configured local stores and explicitly linked private
Threadlane contexts. Linking registers an observation reference; it does not pair
a new chat, enroll an MCP connection, change an existing context, or start a
host. It does not scan provider conversation histories. A failing source is
reported alongside healthy sources instead of hiding their work.

A linked context must pass the same private path/file checks used for local
observation. Never paste a credential or a raw provider conversation into the
context path field. The context's executable pin is not run to read history.

## Retention and limits

New requests are recorded at supported Threadlane CLI/MCP/API boundaries. This
is not a global monitor of arbitrary AGY, OpenCode, or Codex work outside
Threadlane. Older worker artifacts can supply historical execution evidence,
not an invented historical follower request.

Reads are bounded. Request history is paginated; job/history caps and unreadable
or partial sources produce visible warnings. The journal reader uses a trailing
16 MiB window and warns when older journal records are outside that window.
Records are retained on disk; this release does not provide an archive browser
for every byte of older journal history. Missing usage, caller identity, or
provider effort stays explicitly unknown.

Linux fixture tests and browser component checks do not establish macOS/Windows
native file security, keychain, rendering, or live provider compatibility. Native
visual and end-to-end acceptance must be reported separately from fixture checks.
