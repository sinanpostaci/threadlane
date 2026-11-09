# Threadlane implementation plan

Threadlane is the user-selected product name (2026-10-29). Rust + Tauri is the
selected technology. The approved desktop direction is a compact React/Radix
workspace: a collapsible projects/chat sidebar and a selected-chat activity view,
with neutral surfaces and progressive disclosure. README documentation distinguishes
planned capabilities from qualified behavior.

## Product contract

The active first-use scope, prioritized fixes and acceptance gates are recorded
in [First personal MVP acceptance](MVP-READINESS.md). Older checkpoint sections
below describe their original evidence and may predate the current role driver.

The user stays in the native Codex conversation, including frequent Steer and
questions. Codex interprets intent and plans. Threadlane owns validated execution,
durable events, profiles, process supervision and observation. It does not grant
itself new scope, select an unapproved model, or replace the user's conversation.

Two paths must work: worker -> Threadlane -> Codex -> user; user -> Codex ->
Threadlane -> affected worker. Software owns identity/correlation and lifecycle.
Everyday work stays in Codex; the desktop is an optional observation and settings
surface. Trusted approval of a new execution scope remains explicit, but ordinary
task dispatch and follow-up must not require a panel visit for each action.

## Milestones

1. **Foundation and conversation loop.** Rust core, SQLite state, authenticated
   local supervisor, a small MCP adapter, deterministic test worker and Codex
   wake-up adapter. Demonstrate progress, a question, answer, Steer, cancellation,
   duplicate safety and restart recovery in an isolated disposable chat.
2. **Real workers and Git isolation.** OpenCode v2/Go and Antigravity adapters;
   requested/observed models and effort; per-task worktrees, per-run integration
   worktree, scoped diff gates, bounded/paginated run history and compact mutation
   responses, process-tree supervision and evidence acceptance.
3. **Desktop panel.** Tauri with React/Radix, Projects/Tools/Profiles/Settings,
   selected-chat activity, compact job summaries, system theme and accent. A
   modal navigation drawer supports small windows. Core execution and observation
   contracts remain independent of the UI library.
4. **Profiles and Jev.** Global tool/model/effort registry and configurable role/
   difficulty assignments. Jev is included as an API integration; start in shadow
   mode for output classification and missing-evidence flags. Keep a small
   decision-provider boundary for later OpenAI Decisions support.
5. **Release hardening.** Linux lifecycle and tray qualification, Nix packaging,
   measured cost and notification budgets, crash recovery and compatibility checks.
   Keep shared macOS preparation inexpensive; macOS acceptance follows the MVP.
   Windows implementation and acceptance are deferred.

Linux/NixOS is the first supported test host. Shared core boundaries support future
platform work without implying that untested macOS or Windows builds are supported.

## Current checkpoint

The managed Linux desktop route now exposes a no-argument
`connect_current_chat` MCP operation. It checks the calling process's parent
endpoint and thread-scoped connection, preserves an existing durable binding,
and starts only the baseline control runtime when needed. Automatically connected
scoped calls require matching host-provided thread metadata. Unit/protocol checks
cover missing/forged hints and concurrent context isolation; installed desktop
automatic connection, reconnect after reboot, and non-Linux acceptance remain
separate live gates. See [native pairing](NATIVE-GATE.md).

The [native operator commands](NATIVE-OPERATOR.md) expose existing scoped plan
and approved-session operations to a trusted local caller. They preserve human
scope approval, explicit host startup and review acceptance. This route does not
require a desktop form or native MCP connection and does not establish native
per-call identity or automatic same-project two-chat routing.

The [2026-10-31 operator CLI pilot](NATIVE-CLI-PILOT-2026-10-31.md) records one
Linux/NixOS trusted local conversation with a real busy-turn callback, a six-event
pull/ACK, cancellation of a real OpenCode process, and explicit stop/restart
without replay. Cancellation did not prove model output, effort telemetry, or
remote inference termination, and no code was accepted.

The same operator route provides explicit owner/instance-scoped idle stop and
truthful stopped-session inspection. An opt-in
[CLI notification route](NATIVE-OPERATOR-NOTIFICATIONS.md) uses a fixed message
through the existing Codex queue, with no shell adapter or worker prose.

The additive [native tool-output transport](NATIVE-TOOL-OUTPUT.md) is a separately
selected operator route over a private Unix WebSocket. Controlled Linux desktop
experiments verified idle wake-up and delivery into an existing busy turn in one
authorized disposable chat without converting the machine event into user text.
The [qualified product pilot](NATIVE-TOOL-OUTPUT-PILOT-2026-10-31.md) subsequently
verified one real DeepSeek Go task, a native Luna pull/ACK cycle and scoped
teardown. Its launcher is available as an optional Linux Nix helper. Restart
recovery, concurrent human steering, general multi-chat identity and other
operating systems retain separate acceptance boundaries. Legacy queue
notifications are not an automatic fallback.

The [2026-11-01 installed-package check](NATIVE-TOOL-OUTPUT-RESTART-2026-11-01.md)
verified manually reselected delivery after human activation and restart, with a
human-confirmed response in the same native chat. It does not implement automatic
endpoint refresh or promote a transport receipt into event handling or approval.

The subsequent opt-in [Linux reconnect policy](NATIVE-TOOL-OUTPUT.md#automatic-endpoint-refresh-on-linux)
adds automatic managed-endpoint selection and bounded pre-effect connection
recovery while an approved host is running. It preserves exact chat/settings
checks and the no-replay rule after a possible send. Host restart after reboot,
indefinite app-closed delivery, general identity and other platforms remain open.

Milestone 1's deterministic native conversation loop and Milestone 2A's bounded
history/replay contract passed their Linux/NixOS gates. The Milestone 2 worker
library and diagnostic CLI provide provider evidence parsing, bounded process
supervision and owned Git worktree/diff diagnostics independently of the runtime.

Milestone 2B now connects that foundation to durable worker dispatch over scoped
HTTP/CLI/MCP, with bounded live progress, immutable profiles, cancellation and
explicit restart uncertainty. Its primary-reviewed fixture gate passed 82 tests
and independent process/migration/artifact probes. See [dispatch](DISPATCH.md).

Milestone 2C adds operator-selected, finite AGY task-file grants while preserving
legacy default profile identity. Its 89 Rust-test gate passed. A qualified native
single-chat pilot now verifies start, real isolated file editing, return/wake-up,
event acknowledgement and separate deterministic cancellation. Missing real
provider effort telemetry correctly prevented acceptance.

Milestone 3A now adds the first narrow desktop preview: a read-only runtime
observer, a separately owned local demo, recorded task/profile inspection, passive
CLI discovery, and persisted appearance preferences. The standalone Linux build
and native Wayland rendering passed on the first host with the documented
process-local NVIDIA compatibility flag. See [desktop usage](DESKTOP.md).

Milestone 4 now adds editable global tools/profiles, durable plan publication,
immutable operator approvals, separate local hosts, frozen root-task dispatch,
and per-approval observation. See the [M4 checkpoint](M4-CHECKPOINT.md) for its
fixture gates and qualified limits.

The [Jev shadow advice package](JEV-SHADOW.md) adds an explicit
`threadlane-advice` CLI and a fourth `advice` context family. `preview` performs
no network or vault access, `models` reads the account catalog without importing
it, and `classify` makes one advisory Jev Choice request or returns a completed
cache hit. Advice never changes plans, profiles, difficulty, models, approvals,
hosts, workers or Git. Offline fixtures and simulated HTTP responses cover the
advisory lifecycle. One explicit Linux catalog GET with a GUI-entered key
returned `jev-latest` and `jev-preview`; live classification, paid entitlement
and cross-platform keychain acceptance remain separate gates.

[Profile decision advisors](PROFILE-ADVISOR.md) add an optional shadow-only API
model selection independently of the five execution roles. The desktop editor and
native profile catalog expose the same selection; explicit `advice --profile-id`
calls require an exact request/profile match. No request is sent by editing or
saving a profile, legacy off profiles preserve their original wire shape, and
running approvals stay frozen. OpenAI remains a configuration extension while
the current advice execution route supports Jev only.

The [approved-session notification extension](APPROVED-NOTIFICATIONS.md) adds
default-off startup policy, fixed approval wake-ups and transactional byte-bounded
event pull/ACK through the original paired native enrollment. The
[2026-10-31 operator CLI pilot](NATIVE-CLI-PILOT-2026-10-31.md) records one live
delivery and busy-turn callback in its single conversation; fake-process fixtures
and that pilot still do not establish general multi-chat delivery.

Provider interactive continuation and the [multi-chat routing gate](NATIVE-ROUTING.md)
remain separate integration work. General multi-chat notification delivery,
product API entitlement, and cross-platform release hardening remain open. See
[validation](VALIDATION.md) and the
[operator CLI pilot](NATIVE-CLI-PILOT-2026-10-31.md).

The optional [Codex-first startup extension](CODEX-WORKFLOW.md) removes the
desktop Start step for an existing approved root task. Trusted scope approval
uses explicit desktop confirmation or the separate operator-only
[native approval CLI](NATIVE-APPROVAL.md); its flags are operator attestations,
not authentication or proof of human conversation provenance. The OpenCode v2
worker now integrates an [owned-session metadata collector](OPENCODE-METADATA.md)
without changing requested-versus-observed evidence or automatic-acceptance boundaries.
A [qualified native single-chat pilot](NATIVE-PILOT-2026-10-30.md) records that
earlier simulated loop and its remaining identity/onboarding gates. The later
[2026-10-31 operator CLI pilot](NATIVE-CLI-PILOT-2026-10-31.md) records busy-turn
delivery without generalizing it to multi-chat routing.

The [trusted operator CLI](OPERATOR-WORKFLOW.md) exposes the existing task
commit, review acceptance and same-run integration operations to an explicit
local caller. It adds no automatic acceptance, primary merge/push, dependency
input assembly, native enrollment or MCP authority. The subsequent
[dependency-input extension](DEPENDENCIES.md) adds explicit receipt-bound
fan-in and chained execution using accepted predecessor task heads. It keeps
preparation, dispatch, review, main publication and cleanup as separate actions;
no follower-to-orchestrator consultation automation is implied.

The additive [native scope approval CLI](NATIVE-APPROVAL.md) separates a
read-only `prepare` preview from a digest/thread/operator-confirmed `record`
that reuses the existing authority validation and ledger format. It requires a
live configured binding, rejects stale or changed previews, and does not change
the MCP surface, start execution, merge or push. The flags are local operator
attestations, not proof of the human source of an approval; automatic
conversation monitoring and general multi-chat routing remain separate gates.

Read-only [Codex CLI advisory workers](CODEX-ADVISORY.md) extend explicit
post-preflight normalization to `Orchestrator`, `Architect`, `Reviewer`, and
`Closure` roles while keeping generic `RoleExecutionRoute::from_adapter(CodexCli)`
and manual role-flow handoffs on `NativeHandoff`, keeping the native Coworker
conversational-only with no model profile slot or lock, and keeping headless
`Coder` Codex execution unsupported. Paired MCP Activity records also separate
runtime-observed actor telemetry from bounded once-per-call verified thread
`selected_model`/`selected_effort` metadata; see
[Coworker observation](COWORKER-OBSERVATION.md) and
[Coworker model observation](COWORKER-MODEL-OBSERVATION.md). Source-free CLI
framing probes and local fixtures do not claim installed desktop or live
end-to-end provider acceptance.

## Default workflow example

Profiles remain configurable across the executable worker roles (`Orchestrator`,
`Architect`, `Coder`, `Reviewer`, and `Closure`) using explicit registry tools,
exact model/effort targets, and deny-by-default execution policies (see
[profile flow](PROFILE-FLOW.md) and [Codex advisory workers](CODEX-ADVISORY.md)).
The native Coworker remains the human-selected Codex conversation, requires no
`Follower` profile slot or model lock, and is never dispatched as a worker.
Saved profiles and catalog entries grant no execution authority on their own.
Development staffing and review rules are governed by [`AGENTS.md`](../AGENTS.md)
(active human conversation model/quota restrictions, primary Codex agent
implementation review/validation, and one authorized `gpt-6-astra` `high`
package-closing review).

## Non-negotiable invariants

- Fixed machine-generated wake-ups contain no worker prose; detail is pulled as
  untrusted MCP result data. User permissions have a separate trusted origin.
- Distinguish submitted, received, handled, stopped and accepted. Unknown outcomes
  stay unknown until reconciled. No blind replay of external effects.
- Atomic revision checks prevent old questions/answers/instructions being applied
  to a revised task. Questions alone do not revise a plan.
- Duplicate delivery is possible; commands and effects are deduplicated. Never
  promise exactly-once external transport.
- Independent writers use isolated worktrees; integration has its own checkout.
  Unexpected changes are preserved and reported, never automatically reverted.
- UI closure does not stop workers. There is one supervisor/state-writing owner.
- Reviews are tied to the actual patch/base/scope/policy and not needlessly repeated.
- Costs and provider usage estimates must be labelled. No unmeasured savings claims.

## Existing evidence

On 2026-10-29 an external Python script successfully queued two messages into a
disposable native Codex desktop chat, including from a clean environment. Both
caused completed replies. The CLI was 0.165.0-alpha.1.2 and the desktop was open.
One reply changed the echoed tag: software must own correlation IDs. A later isolated native MCP test completed the deterministic
question/answer/completion loop with both wake-ups and final acknowledgements;
see [validation](VALIDATION.md). Busy-turn, real-worker recovery, app-closed and
cross-platform behaviour remained unverified at that checkpoint. The later
[2026-10-31 operator CLI pilot](NATIVE-CLI-PILOT-2026-10-31.md) records one live
busy-turn callback and bounded pull/ACK without changing the earlier observations
or establishing multi-chat routing.

The independent Argon High and Opus 5.5 High design reviews were completed before
implementation. They motivated the invariants above; their agreement is not
runtime evidence. Their raw input/output remains in the planning workspace.
