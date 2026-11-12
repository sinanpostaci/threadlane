# Desktop observation

The desktop is an optional, read-only observer. Keep planning, questions, scope
approvals, work dispatch, and follow-up in native Codex. Opening **Projects**
(`Messages`, `Jobs`, or `Activity`) does not start a worker, select a model,
approve a task, consume a native follower event batch, write Codex configuration,
or replay work.

## Chat-scoped workspace surfaces: `Messages`, `Jobs`, and `Activity`

Production observation reads one bounded `WorkspaceSnapshot` per refresh via
`desktop_workspace_snapshot` and groups conversations by canonical project scope
under **Projects / Active** and **Projects / Archive**. Every chat is identified
strictly by its verified four-part `WorkspaceChatIdentity` (`source_id`,
`binding_id`, `thread_uuid`, `canonical_project_scope`). Records lacking a
verified four-part identity or conflicting with a chat's scope are withheld and
counted as bounded omissions rather than guessed from titles, project labels, or
`source_id` alone.

Within a selected chat, `WorkspaceChatPanels` separates three distinct surfaces:

1. **Messages (default tab):** chronological safe records (`work_brief_summary`,
   `plan_summary`, `question_prompt`, `question_answer`, `task_result_summary`,
   `review_verdict_summary`, `closure_summary`, `job_result_summary`,
   `attention_summary`, and `journal_summary`). It never reads raw provider
   transcripts, never fabricates synthetic user turns, and includes no message
   composer. User and agent text is rendered verbatim in its original language
   while UI chrome stays compact, neutral, and English.
2. **Jobs:** durable ledger of jobs, tasks, attempts, task candidates, combined
   candidates, attention holds, historical failures, and backend-verified
   acceptance proofs. Several jobs can exist in one chat without merging their
   lifecycles.
3. **Activity:** chronological audit feed of requests, causal child calls, and
   result-delivery timelines for the selected chat (with optional job scoping via
   `View activity` / `View job` across tabs). Progress updates the existing
   request and child calls in place rather than creating rows per stream chunk.

### Independent Coworker state, configured selection, and execution evidence

- **Live Coworker state vs Worker execution:** native chat sampling reports
  whether the conversational Coworker is `working`, `idle`,
  `waiting_for_approval`, or `waiting_for_input` independently of background
  Worker runs. If a native sample exceeds the freshness threshold or disconnects,
  live Coworker activity and selected model are withheld as `Stale` or
  `Unavailable`.
- **Configured chat model vs runtime telemetry:** a verified native sample can
  display the chat's currently configured model/effort selection, explicitly
  distinguished from per-call runtime execution telemetry.
- **MCP calls never complete a chat or job:** local Threadlane MCP reads and
  mutations appear in `Activity` as audit records; a completed MCP call never
  marks the conversation or any job `Completed`.
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
- **Ready for review / verified accepted:** producing output is distinct from
  accepting a candidate. Authoritative task and job acceptance labels require
  backend-verified acceptance proof (`review_verdict_id` / `review_receipt_id`
  for tasks; `combined_candidate_id` + `closure_verdict_id` for jobs).

### Result delivery is a timeline

`Activity` distinguishes `result_ready`, `notification_submitted`,
`batch_delivered`, `batch_acknowledged`, and synchronous `response_returned`.
Notification submission does not prove a chat read it. An ACK proves
acknowledgement of the recorded batch; it does not prove a human saw the result
or that a particular model explained it. A reviewer status does not itself
dispatch a reviewer.

## Navigation, Settings, and source connections

Top-level navigation is **Projects** (with **Active** and **Archive** submenus),
**Tools**, **Profiles**, and **Settings**. Tools owns executable paths,
credentials, and model catalogs. Profiles owns role/model/effort assignments and
the optional Decision advisor. Settings owns appearance, window close behavior,
explicit Codex setup status/apply controls, linked observation sources, and
contextual diagnostics.

The workspace snapshot reads configured local stores and explicitly linked
private Threadlane contexts. Linking registers a read-only observation reference;
it does not pair a new chat, enroll an MCP connection, mutate an existing
context, or start a host, and it never scans raw provider conversation histories.
A failing source is reported alongside healthy sources instead of hiding their
work. Linked contexts must pass the same private path and file-mode checks used
for local observation; never paste credentials or raw conversations into the
context path field.

## Bounded data and verification boundaries

New requests are recorded at supported Threadlane CLI/MCP/API boundaries;
Threadlane is not a global monitor of arbitrary AGY, OpenCode, or Codex processes
outside Threadlane. Production `Projects` loads one bounded `WorkspaceSnapshot`
without interactive `Load older activity` paging or `6h` / `24h` recency filters.
When messages (`<= 1,000`), job histories, child calls, activity requests, or
native samples are capped—or when journal history falls outside the trailing
16 MiB reader window—the UI keeps those omissions explicitly visible rather than
implying complete lifetime history.

### Journal durability and cross-journal append isolation

`append_journal_record` scopes in-process append serialization to each verified
private journal directory identity (`dev+ino` from the `O_NOFOLLOW`-verified
`0700` parent directory handle on Unix; canonical verified directory path on
non-Unix shared builds) through a short-held, bounded weak lock registry
(`<= 1,024` simultaneously active journals). Dead weak entries are pruned on
guard drop and before insertion; if active capacity is exceeded, the lookup fails
closed with an I/O error rather than falling back to process-global
serialization. The registry mutex is never held while waiting on a per-journal
mutex, acquiring the cross-process `fs2` exclusive file lock (`lock_exclusive`),
or executing file/directory sync I/O, so lock contention or `fsync` latency on
one store's `observation-journal.jsonl` does not block appends to unrelated
private stores.

Same-journal writers across threads and processes preserve `O_NOFOLLOW`
parent/file opens, `0700`/`0600` mode, owner, and `dev+ino` revalidation before
and after locking and before completion, interrupted trailing-newline repair,
`flush` + `sync_data`, and Unix parent-directory `sync_all`. These OS sync
primitives provide software-level crash durability only where honored by the
kernel and storage stack; hardware power-cut testing and macOS/Windows platform
acceptance are not claimed.

Source wiring and component fixture tests do not substitute for integrated
combined-checkout checks, wide/narrow Playwright runs, isolated NixOS Wayland VM
tray and Codex navigation acceptance, authorized live workflow qualification, or
macOS/Windows platform verification.
