# Threadlane implementation plan

Threadlane is the user-selected product name (2026-10-29). Rust + Tauri is the
selected technology. The approved narrow, quiet, smoked-glass mockup remains the
visual direction. README documentation will distinguish planned and working features.

## Product contract

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
3. **Desktop panel.** Tauri, a narrow TypeScript UI, Activity/Chats/Tools/Profiles/
   Settings, prompt and event inspection, system theme, accent, adjustable glass
   surface with an accessible opaque fallback. Choose the frontend library when
   this milestone starts. Freeze the event contract before parallel UI work.
4. **Profiles and Jev.** Global tool/model/effort registry and configurable role/
   difficulty assignments. Jev is included as an API integration; start in shadow
   mode for output classification and missing-evidence flags. Keep a small
   decision-provider boundary for later OpenAI Decisions support.
5. **Release hardening.** Early macOS/Windows smoke tests, platform lifecycle and
   secret storage, optional service/tray behaviour, Nix packaging, measured cost
   and notification budgets, crash recovery and compatibility checks.

Linux/NixOS is the first test host, not a product restriction. Jev, glass and
cross-platform support remain in scope. Staging work must not erase those goals.

## Current checkpoint

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
The temporary desktop launcher, product host end-to-end delivery, restart
recovery, general multi-chat identity and other operating systems have separate
acceptance boundaries. Legacy queue notifications are not an automatic fallback.

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

## Default workflow example

Profiles remain configurable. The current desired usage profile is Luna Max for
the Codex follower, Sol 6.1 High for substantive orchestration, Sol 6.1 Max for
architecture, OpenCode Go/DeepSeek 4.1 Flash Max for routine coding and AGY/Argon
Medium for hard coding, Argon High for task review and Sol xhigh for one valid
package closure. This is a future product profile, not the development staffing:
the user requests **Argon High implementation, ordinary review/validation by the
primary agent, and one Astra Max package-closing review**.

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
