# First personal MVP acceptance

This is the current implementation and acceptance plan approved on 2026-11-05.
It supersedes older milestone wording when describing the first usable personal
Linux release. Historical pilot reports remain evidence for their exact scope.

## Supported initial scope

- The current Linux/NixOS computer, with one active project and native Codex chat
  at a time. Independent tasks inside an approved job can run in parallel.
- Native Codex is the conversational Coworker. The human chooses its model in
  Codex. Threadlane profiles configure the execution roles, not the Coworker.
- The desktop is optional for observation and settings. Ordinary approved work
  must not require a panel visit or repeated manual role-routing prompts.
- Planning grants and execution approvals remain separate. Saved model choices
  and worker output never grant authority. Exact provider, model and effort pins
  remain mandatory, with no silent fallback or automatic retry of unknown effects.
- An accepted integration candidate is distinct from merging or pushing `main`.
  Publication and cleanup follow the human's actual authorization.

## Prioritized engineering packages

| Order | Package | Acceptance |
| --- | --- | --- |
| 1 | Historical status reliability | Publishing revision 2 does not break read-only inspection of completed revision 1. Historical integrity stays verified without making the old revision executable. Tampering and wrong-scope reads remain rejected. |
| 2 | Settled-block notifications | Failed workers, failed checks and correction waits produce a durable, deduplicated attention notice without waiting on a cached `Running` sibling; terminal sibling evidence is reconciled under the failure fence so the owner settles cleanly without promoting fenced work. Unknown sends are reconciled rather than replayed. Completion still requires one accepted Closure. |
| Parallel with 1 and 2 | Complete normal validation | Every frontend test file is discovered, failures propagate, and zero discovered tests fail. Standalone desktop Rust tests and Clippy have explicit validation commands. |
| 3 | Persistence and interruption safety | Directory-sync failures are not silently reported as success. Post-rename uncertainty preserves visible state and stops effectful advancement. Restart reuses proven receipts and never blindly repeats provider calls. |
| 4 | Installed workflow acceptance | The exact installed package and approved profile complete a small real job, including parallel roots, a dependent task, verification, review, integration and one Closure. Results return to the original chat. |
| 5 | Fresh-chat and observation acceptance | A new native chat connects and reads its guide/profile without manual role instructions. Activity and Plans show the real job, role/model, state, duration, actual usage or unknown, and delivery/ACK evidence. |

Status, notification and test-command changes use independent worktrees. Shared
persistence semantics are integrated before the final combined validation. Each
checkout owns its own Cargo target directory. The primary reviews changes and
one package-closing review follows the integrated checks.

## What already exists

The implementation includes standing planning grants, bounded Architect advice,
structured Orchestrator plans, frozen approvals, parallel task execution,
dependency inputs, verification, Reviewer calls, bounded corrections, integration,
single-Closure completion, durable receipts, ownership locks and recovery
projections. Activity and Plans already project these records. These components
need qualification together; they are not a reason to rebuild the architecture.

The pre-implementation review confirmed a working installed managed connection
and profile catalog, and reproduced the historical-summary failure. It also
identified seven frontend test files omitted by the standard command; their
73 cases passed when run separately. Those are historical review results, not
the final validation of this package.

## Required acceptance scenarios

1. **Normal job:** from a fresh exact approval, two independent tasks and one
   dependent task use their configured models/efforts, pass checks and review,
   integrate, receive exactly one accepted Closure and report to the same chat.
   Repeated start/status/reconcile requests cause no duplicate provider call.
2. **Human intervention:** a question changes no scope. A real amendment holds
   work at the supported boundary and requires a revised approval. Stop is
   durable, Coders are signalled, finite review/check/Closure work is drained and
   late outputs cannot advance the stopped job.
3. **Interruption:** process loss and reopening preserve completed work and
   identify uncertain effects. Reconciliation inspects actual receipts and does
   not regenerate completed work or resume a provider at an imagined token.
4. **Observation:** every important transition is linked to its actual job and
   call. Requested/configured metadata stays separate from runtime-observed
   evidence. A submission or ACK is never presented as proof of human reading.

Headless AGY/OpenCode live prompt injection and provider pause/resume are not
part of this first acceptance. Use supported checkpoint, stop and replan paths.
Process interruption tests do not establish physical power-loss guarantees.

Live calls, test chat creation, installation/activation and new execution scopes
need their applicable explicit human authorization. Development approval is not
a reusable planning grant or execution envelope.

## Deferred work

General simultaneous multi-chat/project acceptance, macOS managed-route support,
Windows platform implementation, automatic Jev/Decisions integration, advanced
cost forecasting, public signed installers, service/tray expansion and a new UI
design do not block the first controlled personal use.

Early hardening after acceptance includes receipt-preserving history archival
and capacity visibility, complete automatic Linux PR checks, reservation-aware
aggregate budgets, and privacy-safe backup/restore. Current bounded ledgers must
never be described as unlimited long-session storage.
