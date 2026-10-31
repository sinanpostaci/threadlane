# Foundation validation

Verified on 2026-10-29 on Linux/NixOS. This is a checkpoint report, not a claim
that every milestone or platform is supported.

## Packaged operator context checkpoint (2026-10-31)

The context command package passed **360 workspace tests** with the existing
serial CI harness (`cargo test --locked --workspace -- --test-threads=1`) and
**23 desktop tests**. Workspace and desktop Clippy passed with warnings denied;
both formatting checks passed. Fixtures use local supervisors and fake children,
not paid providers. Context validation and forwarding do not grant human approval
or authenticate native per-call identity; see [context commands](NATIVE-CONTEXT.md).

One earlier default-parallel workspace run failed the existing proxy overflow/
timeout process fixture. The isolated test, its native-library group, and the
complete serial workspace recheck passed. Its expected error-code guard and
process-group cleanup assertions remain intact; a static failure-code diagnostic
was added. The cause of that single parallel failure is not established, so it
remains a fixture follow-up rather than a claimed production fix.

The [qualified native CLI pilot](NATIVE-CLI-PILOT-2026-10-31.md) separately records
the earlier live callback, actual six-event ACK, local cancellation and explicit
restart without replay. Those observations do not establish automatic native
onboarding, authenticated two-chat routing, API entitlement or other platforms.

## Owned OpenCode execution evidence (2026-10-31)

Focused tests cover the bounded metadata collector, owned worker execution,
parameterized failure cases and runtime completion mapping.
The existing 24 worker/Git foundation regressions also passed before the live
probe. All 21 host tests also passed after adapting their shared fake CLI to
metadata-only commands without counting them as inference. Fixtures retain the
documented scalar `outcome` schema and actual CLI argument ordering; the validator was not weakened to accept a failing fixture.

One disposable live task on candidate `a36bd0b` used OpenCode 2.2.2 with
`opencode-go/deepseek-v4.1-flash#max`. It completed in 15.34 seconds, changed only
`result.py`, passed the diff gate and remained
`diff_gate_passed_pending_review`. The generated addition function matched the
expected syntax tree and passed two arithmetic checks. The source fixture's main
checkout stayed clean; no patch was accepted or merged.

Per-assistant sanitized metadata confirmed the exact owned session, requested
model and `max` effort. The report retained `saw_result_event: false` because the
stream never emitted that evidence. No raw export was persisted. This is
CLI-reported evidence from one bounded Linux task, not cloud attestation, long
session qualification or a live approved-host-to-native-chat workflow.

The single closing review found two reporting/state issues. Follow-up regressions
preserve already verified stream fields on unrelated metadata failures, report
finite metadata errors accurately, and require terminal completion proof before
the legacy pending-effort state. These corrections use synthetic/fake-process
checks; the paid live task was not replayed.

The separate [native single-chat pilot](NATIVE-PILOT-2026-10-30.md) records its
qualified simulated loop and restored temporary configuration. Automatic native
identity/onboarding, two-chat isolation, provider continuation and verified DAG
inputs remain separate acceptance gates.


## Approved native startup checkpoint (2026-10-30)

The integrated Linux candidate passed `cargo test --locked --workspace
--all-targets -- --test-threads=1`: **264 tests**, zero failures or ignored cases
across 40 test binaries. Workspace formatting and Clippy with warnings denied
also passed. The serial harness setting isolates timing-sensitive fixtures;
the explicit concurrent-start cases still exercise their own parallel calls.

Coverage includes default-off startup policy, CLI prerequisites, unchanged MCP
tools, frozen scope/task checks, enrollment revocation, exact process arguments,
fresh-only preservation, cross-coordinator launch intents, duplicate dispatch,
bounded readiness and cancellation without late process creation. The real host
CLI lifecycle regression verifies authenticated stop/restart and no task replay.

A repeated native check exposed a real work-area initialization race: the first
worker could create an ownership lock before its marker, while a second task's
authority validation observed the temporary lock-only directory. Retrying an
isolated test did not establish a fix. In a disposable synthetic snapshot, the
primary inserted a 300 ms scheduling pause after the existing work-area lock
acquisition and reproduced `invalid_work_area` with the original assertions.
The host now initializes and verifies its repository-bound work area before
Ready. The same widened-window scenario then passed. Neither diagnostic logging
nor the scheduling pause is included in product source. Additional regressions
verify private marker/directories and zero workers before dispatch, and preserve
a Stopped manifest when its work-area marker refers to a foreign repository.

The single closing review exposed two additional startup defects even though
the earlier 261-test candidate and its Nix build had passed: blocking authority
and enrollment checks could exceed the deadline and permit a late spawn, and
pre-manifest observer probes could compete with the starting owner's host lock.
That candidate was held from main. The corrections bound read-only validation
and enrollment off the async executor, persist intent through a bounded
write-only handoff, and leave process spawning with the live request after
current deadline/enrollment checks. Cancellation retains conservative intent
evidence without giving detached jobs a process-spawn capability. Initial
inspection, locked preparation and polling now share the intent-aware guard.

Three committed regressions compile against the rejected candidate and fail on
its authority wait, final enrollment wait and stage-gated caller cancellation;
all pass on the corrected source. The cancellation window starts only after the
contended callback actually holds the pairing lock, independently of the longer
startup deadline. The targeted closing recheck also passed four disposable test
functions covering six cases: both real lock-contention deadlines, timeout and
after-entry cancellation during a 700 ms Pending-write/fsync delay, and both
pre-manifest inspection entries. The 250 ms cases returned in 250-251 ms with no
late process; both lock entries reached authenticated Ready with one owner and
no replacement. These additional instrumented probes are separate from the
264-test total; their scheduling hooks are not product source. No second broad
closing review was run.

The Tauri workspace passed **23 tests**, formatting and Clippy. Its production
Rust and Cargo inputs were compared with the final candidate after the final
cancellation-test refinement; only that test module differs from the Tauri
snapshot, and all compiled production inputs remain identical.
Frontend inputs are unchanged from the existing 70-test/typecheck/build gate.
The built frontend assets were reused rather than running an unchanged suite
again. The standalone embedded-asset fix separately passed 11 Python tests and
an exact-commit Nix build; the user confirmed its native window with the
temporary port 1420 server stopped.

The pure OpenCode v2 metadata validator contributes 13 synthetic tests to the
root total. It is not connected to the runtime collector and does not establish
live Go worker acceptance. See [metadata boundaries](OPENCODE-METADATA.md).

This gate establishes local fixture and build compatibility. New-scope approval
still uses desktop confirmation, and real two-chat/busy-turn delivery, live
interactive provider continuation, receipt-bound DAG inputs, product API
entitlement and macOS/Windows acceptance remain separate. See the
[native conversation workflow](CODEX-WORKFLOW.md). Package-closing review and
the exact-candidate Nix build remain release gates; test counts alone do not
establish either outcome.

## Reproduce locally

From the repository root:

```sh
nix develop
cargo build --workspace
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/smoke.py
```

The committed Nix lock resolves Rust/Cargo 1.99.0. `Cargo.lock` pins the Rust
dependencies, including the official `rmcp` SDK at 3.8.0. The development shell
was actually opened; the checks below ran inside that shell.

## Original foundation checkpoint

| Check | Observed result |
| --- | --- |
| Core behavioural tests | 12 passed |
| Core review regressions | 6 passed |
| HTTP, CLI, stdio MCP, and fake-Codex integration | 13 passed |
| Independent runtime regressions | 3 passed |
| Formatting and Clippy with warnings denied | Passed |
| CLI smoke with real local processes | Start, question, event delivery, acknowledgement, answer, completion, and idle wake state passed |

The original independent regressions initially reproduced failures and passed after
implementation fixes. They cover acknowledgement races, an empty-batch boundary,
an old HTTP handler writing after its supervisor released ownership, symlinked
parent directories, and a never-ending chunked response exceeding the byte budget.

The runtime integration test also injects SQLite failures in an isolated fixture.
The service reports a fault, rejects later operations, and does not repeat an
external wake-up when recording its outcome fails. The stdio test initializes a
real MCP process, lists and calls all eight tools, rejects injected scope/time
arguments, and verifies that cross-binding requests have no effect.

The closure pass adds coverage for escaped instructions through start/steer,
completion and restart, additive v1 schema recovery, manual wake retry without
cursor changes, stale claim rejection and durable retry history. Runtime checks
cover offline MCP discovery followed by recovery in the same process, descriptor
refresh/security, preserving unrelated directories and nonzero CLI exit on a
fatal SQLite error. All 34 tests passed. A redundant `mut` in a new test was then
removed to satisfy Clippy; formatting, Clippy, build and the CLI smoke all passed.

## Native Codex experiment

**The deterministic native conversation loop passed on Linux/NixOS on 2026-10-29.**
The test used one disposable native Codex chat with explicit user authorization.
No other project chat or model, effort, approval or sandbox setting was changed.

Initial attempts delivered the fixed wake-up but the open chat reported
`MCP_UNAVAILABLE`. Public `config/read` diagnostics showed that its project-local
MCP configuration was disabled because the project was not trusted. A temporary,
user-approved trust entry enabled that layer. A standalone installed Codex
app-server could then discover all eight tools; that alone was not native proof.

After the user restarted the desktop app, its app-server launched the scoped
Threadlane MCP process for the test project. Although the user still did not see
it in the `/mcp` menu, a new turn in the existing test chat successfully called
`threadlane_probe.status`. The menu observation did not establish tool failure.
No NixOS package, library or system configuration fix was needed for this test.
The complete reason for the earlier UI omission was not established.

The final experiment independently verified native chat tool records and runtime
state, not only the exit status of the queue command:

1. The native chat cancelled the old waiting demo through MCP, pulled its four
   events and acknowledged the returned batch.
2. It started a new deterministic demo with `start_demo`, then ended its turn.
3. The supervisor was restarted with wake-up dispatch allowlisted to that same
   disposable chat. The already running MCP process recovered with the new private
   descriptor. The waiting run persisted across this restart.
4. The real queue adapter delivered the fixed question wake-up. The chat used
   `pull_events`, `ack_events`, `get_run` and revision-matched `answer` through MCP.
   Its answer was a preauthorized synthetic test response, not real task approval.
5. A second fixed wake-up delivered completion. The chat used `pull_events`,
   `ack_events` and `status` through MCP and confirmed the run was completed.
6. Final independent CLI status showed `active_runs = 0`, `delivered_seq = 9`,
   `acked_seq = 9`, wake-up `idle` and no last error. The cumulative submission
   count rose from one earlier failed-discovery experiment to three: two additional
   wake-ups for the accepted question/completion loop.

The owned supervisor/MCP processes and only the owned temporary trust/project
configuration entries were removed after the test. No persistent service was
installed, and unrelated Codex configuration was preserved.

To repeat in a dedicated trusted test project, configure the scoped stdio command
and verify a real native `status` call before running the loop:

```text
<absolute-threadlane-binary> mcp --state-dir <canonical-private-state-dir> --binding <test-binding-id>
```

Discovery works with the supervisor offline; actual calls require the supervisor.
A project configuration can be shared by chats in that project, so this fixture
was deliberately restricted to one chat. General per-chat routing remains later
integration work. Busy-turn steering and real external worker/process recovery
were not established by this deterministic native test.

Client setup reference: [Codex MCP configuration](https://learn.chatgpt.com/docs/extend/mcp).

## Deliberate limits

- Durable HTTP/CLI/MCP worker dispatch passed the Milestone 2B fixture gate below.
  Native real-worker interaction, Jev and the Tauri UI remain later work.
- The demo proves durable logical transitions, not recovery of real worker processes.
- macOS and Windows have not been tested. Windows runtime startup fails closed
  until suitable private file ACL support is implemented.
- Run/history requests and compact mutation replay are bounded in Milestone 2A.
  Total lifetime disk retention remains uncapped; source history and idempotency
  keys are not silently deleted.
- Secret files are private to the OS user. This does not isolate a malicious
  process already running as that same user.

## Milestone 2A follow-up (2026-10-29)

The primary agent ran 40 tests (17 core behavioural, 6 core review, 14 runtime
integration and 3 runtime review), Rust formatting, Clippy with warnings denied,
and the CLI smoke. All passed after implementation corrections.

Independent temporary-fixture probes also passed:

- After 40 Steer operations with 4,096-byte instructions, HTTP response size
  remained 4,535-4,537 bytes. The baseline grew from 8,708 to 176,382 bytes.
  Stored dedup responses fell from 3,794,240 to 185,999 bytes in this exact case;
  these figures are not a general provider-token or cost estimate.
- A database created by the actual old v1 binary upgraded to v2. Original Start
  and Steer retries returned revisions 1 and 41 after further changes, with one
  run and all 42 revision records preserved and readable across three pages.
- A real stdio MCP client discovered all 11 tools and read 81 escaped-text
  revisions across 11 pages. The largest serialized RPC response was 244,510
  bytes against the 262,144-byte page budget. An append after page 1 was excluded
  from that existing high-water-mark window, without duplicates or lost rows.

These probes use disposable state and do not invoke paid models or user chats.
Schema/API details and deferred retention are recorded in [BOUNDED-DATA.md](BOUNDED-DATA.md).


## Milestone 2 worker diagnostic checkpoint (2026-10-29)

After merging the independent history and worker branches, the primary agent ran
all 60 tests on the integrated workspace: 17 core behavioural, 6 core review,
14 runtime integration, 3 runtime review and 20 worker/process/Git regressions.
Formatting, Clippy with warnings denied, the locked workspace build and the
process-based CLI smoke all passed. The Nix lock was unchanged; the development
shell now also supplies Git for disposable repository fixtures.

The worker tests exercise exact argument construction, model/effort evidence,
permission blocks, malformed/oversized output, timeouts, cancellation, aborted
supervising futures and bounded cleanup of pipe-holding descendants. Git fixtures
cover source repositories that are themselves linked worktrees, unrelated-folder
preservation, ignored/untracked files, renames, deletes, binaries, symlinks,
protected paths and refusal to delete dirty/active/unexpected worktrees.

Additional primary-agent probes confirmed:

- An unrelated pre-existing directory retained mode `0755` and its sentinel file
  when initialization was refused. No private work-area ownership was fabricated.
- An OpenCode prompt beginning with `--auto` followed the `--` option terminator;
  its text did not become a command-line option. OpenCode requires standalone
  mode so the runner does not attach to an unowned shared backend.
- Two distinct Unix filenames, valid UTF-8 `U+FFFD.rs` and raw bytes `ff 2e 72 73`,
  no longer collapse onto one allowed path. The independent probe now reports
  `passed = false` with two violations. The regression also verifies that the
  valid Unicode filename alone passes and that entry/depth budget exhaustion
  rejects incomplete scans. No fixture invokes a model.

One live Antigravity transport smoke used the requested `gemini-4-argon-high`
model with `--effort high` in a throwaway owned worktree. Real init/result events
confirmed the model, but the stream did not report effort. The runner therefore
returned `missing_effort_evidence`; work was not automatically accepted. The
prompt requested only a fixed reply with no tools or edits. The unchanged diff
passed, the source HEAD was preserved, and the owned task/integration worktrees
and branches were removed afterward. This is transport evidence, not a real
coding-task acceptance test. No live OpenCode/Go model was invoked; OpenCode was
checked against installed CLI help and deterministic fixtures.

At this diagnostic checkpoint the worker library and `threadlane-worker` CLI
were not yet connected to durable MCP worker dispatch; the later Milestone 2B
checkpoint below supersedes that integration status. Durable worker state/recovery, bidirectional native
Steer/question/cancel, reviewed merge/cleanup of accepted results and the desktop
UI still need their own integration checkpoint. Diff checks are post-run
inspection, not an OS filesystem sandbox. macOS and Windows remain untested.
Raw provider logs and runtime data remain private and outside version control.


## Live worker observation follow-up (2026-10-29)

Four additional fake-process regressions passed: progress is observed before
exit, full/closed channels preserve terminal evidence and count drops,
timeout/cancel works with a full observer, and disabled observation preserves
existing results (including honest missing Argon effort evidence).

The integrated workspace passed all 64 tests with
`cargo test --workspace --locked -- --test-threads=1`, formatting, Clippy with
warnings denied, build and CLI smoke. The default parallel harness attempt did
not pass: two existing fake-executable tests received OS `ETXTBSY` / `Text file
busy` at spawn. No behavior assertion was removed or relaxed. The single-threaded
harness is the verified command for this checkpoint; its worker concurrency and
process-cancellation regressions still exercise concurrent processes explicitly.
The precise cause of the intermittent parallel fixture failure is unresolved.
The standalone HTTP/stdio review fixtures were also verified against the actual
supervisor and all 11 current MCP tools, without paid models or user chats.

The observer follow-up alone did not wire real workers into durable dispatch;
the subsequent checkpoint below adds that connection.


## Milestone 2B durable dispatch checkpoint (2026-10-29)

The primary agent reviewed Argon High implementation and independently ran all
82 workspace tests: 32 core, 26 runtime and 24 worker/process/Git tests. The
verified command is `cargo test --workspace --locked -- --test-threads=1`.
Formatting, Clippy with warnings denied, the locked workspace build and the
CLI smoke all passed. The earlier parallel-harness `ETXTBSY` limitation remains
unresolved; this report does not claim that command passed.

The integrated runtime now exposes 14 MCP tools. Added fixtures verify disabled
startup, side-effect-free private profile validation, forbidden overrides,
binding isolation, exact base/digest identity, idempotent admission, configured
parallelism, distinct task worktrees, live progress before process exit, bounded
and redacted payloads, cancellation/completion races, stale callbacks, fault/drop
and restart recovery without automatic worker replay.

Additional primary-agent probes against the built HTTP/stdio runtime passed:

- Actual process overlap was exactly 1 and 2 under the respective limits, using
  disposable repositories with unchanged source checkouts.
- A real stdio MCP client discovered 14 tools, observed pre-exit progress, replayed
  duplicate admission without a second process, and cancelled with a current
  revision. The same MCP process recovered its descriptor after supervisor restart.
- A supervisor killed during a fixture worker reopened with an explicit `Unknown`
  outcome and no automatic re-invocation. This does not prove an old process died;
  the fixture separately cleaned up only its own verified process.
- A database created by the actual prior schema-v2 binary upgraded to v3. Original
  idempotent Start/Steer snapshots and both revision records remained exact, and
  the newly introduced worker table was empty.
- Maximal escaped instructions and path previews remained readable at terminal
  state. The independent core regression first reproduced an oversized terminal
  snapshot (106,564 bytes against 65,536), then verified the correction: public
  optional previews shrink to fit while original instructions, counts, status
  and complete bounded outcome data remain in SQLite.
- Report-directory symlinks, artifact-file symlinks and insecure existing report
  directories retained their original contents and modes. The report was omitted
  and the genuine fixture worker still completed. The committed regression also
  covers an existing regular artifact and successful private `0700`/`0600`
  creation with cleared prompt argv.

Review corrections included an invalid SQLite hidden-rowid index, a response
budget failure that could prevent terminal persistence, runtime API mismatches,
and report writing that could follow or chmod pre-existing paths. Two new tests
were corrected to use the actual stale-claim and MCP parameter-error contracts;
the behavioral rejection/preservation assertions remain in place.

No probe invoked a paid provider or another user chat. Development coding used
the explicitly selected Argon High; this is not evidence that a real worker task
was accepted through the product. The earlier live AGY transport still lacks
observed effort, and live OpenCode/Go remains unverified. Native real-worker
Answer/Steer, busy-turn delivery, automatic multi-chat binding, reviewed merge/
cleanup and the desktop UI have not passed their acceptance gates. Provider
permissions are not bypassed. See [DISPATCH.md](DISPATCH.md) and
[NATIVE-ROUTING.md](NATIVE-ROUTING.md). Private raw logs, temporary binaries and
fixture state are excluded from Git.


## Milestone 2C scoped permissions and native pilot (2026-10-29)

The primary ran 89 Rust tests (32 core,27 runtime,30 worker/process/Git) with
`cargo test --workspace --locked -- --test-threads=1`. Formatting, Clippy with
warnings denied, the locked build and deterministic CLI smoke all passed. The
existing parallel fixture-harness limitation remains unchanged. Added coverage
exercises actual task-directory settings argv, default/strict policy serde,
OpenCode rejection, literal-path/rule validation, Unicode/spaces, existing target
symlink rejection, grant-count/actual-JSON-byte ceilings, immutable policy identity
and unchanged post-run scope/evidence gates.

A separate probe created state with the actual prior M2B binary and restarted it
with the candidate. An unchanged default Inherit profile retained its exact hash;
retry returned the exact original StartWorker snapshot and launched no second
process. Opt-in TaskFiles produces a different hash. This catches an upgrade
compatibility issue found during primary review without weakening deduplication.

Native evidence was checked against public desktop chat/tool records and private
runtime state, not queue exit status alone:

1. A fresh authorized disposable same-directory chat fork called real MCP `status`;
   software-owned binding/thread identities matched. Project configuration and
   private startup binding were prepared only for this test scope.
2. The native chat called `start_worker` exactly once for a fixed Argon High profile,
   then `get_worker`. The worker used file tools to change only `greeting.txt` from
   `Hello` to `Hello from Threadlane` in its owned task worktree. The source HEAD,
   status and original greeting were unchanged.
3. The real provider emitted 18 durable live progress events, correct observed
   model, exit 0 and a passing one-file diff. It did not emit effort. The runtime
   correctly recorded Failed/ExecutionFailed with `missing_effort_evidence`, not
   ReadyForReview or Accepted. Requested high is not observed-high proof.
4. One fixed terminal wake-up reached that same native chat. It pulled/ACKed all 21
   events and inspected the actual terminal worker outcome through MCP.
5. A separate deterministic sleeping local process (no second model API call)
   verified native start/get/revision-checked cancel, then terminal wake-up,
   pull/ACK/get. Proven status was Cancelled with signal 15, no changed files and
   no incomplete cleanup. Final binding delivery/ACK cursors were both 27, with
   wake-up Idle and `active_runs = 0`.

The temporary test-project trust/MCP entries and only owned supervisor/MCP
processes were removed. Unrelated global model/effort/security settings and other
chats were preserved. The initial projectless test chat used a separate directory
and had no MCP; it was archived. The accepted same-directory fork is identified
in private probe evidence. No raw conversation, provider log or real chat ID is
committed. Dirty fixture task worktrees stay private for inspection; they are not
force-cleaned or merged into product code.

This is a qualified native coding/return and cancellation transport checkpoint,
not full provider-effort/patch acceptance. Live OpenCode/Go, same-project
multi-chat authenticated identity, busy-turn delivery, interactive provider
Answer/Steer, reviewed result integration and the Tauri UI remain separate gates.


### Operator harness verification

Seven Python unittest cases passed after the harness was simplified and corrected
to the real runtime contract. They cover no-effect dry-run, CLI argument bounds,
pre-existing/symlink output preservation, evidence classification/whitelisting,
private fixture output and a deterministic fake local supervisor. The fake
executables use the actual Python interpreter path, so tests do not rely on an
ambient `python3` shebang being available on NixOS.

The primary additionally ran the committed harness against the actual built Rust
runtime and two deterministic local provider processes (no provider API calls):

- Full synthetic model/effort evidence: exit 0, Completed/ReadyForReview, expected
  edit and source preservation; `patch_accepted` stayed false.
- Missing synthetic effort: exit 1, Failed/ExecutionFailed, missing-effort evidence
  retained, expected edit/source preservation still verified. It was not promoted
  to success because code existed.

Both cases retained private 0700 output roots and 0600 config/summary files. The
pre-fix real-runtime probe caught a discrepancy missed by the mocked-supervisor
unit fixture: actual startup JSON omitted TaskFiles despite the dry-run advertising
it. Primary review also corrected invented lifecycle/acceptance/event labels and
normal help handling. The fixed harness now sends TaskFiles and preserves actual
ExecutionFailed evidence. This operator utility is not native-chat acceptance;
the separate native experiment above supplies that qualified transport evidence.


### Closing-review Git environment correction

The Astra Max closing reviewer reproduced a blocking operator-harness issue using
only disposable repositories: inherited `GIT_DIR`, `GIT_WORK_TREE`, index and
config-injection variables could redirect fixture Git commands into an unrelated
repository and commit its staged change. The primary delegated the targeted fix
to Argon; the harness now builds one sanitized environment for Git, Threadlane CLI
and owned supervisor children, removing inherited `GIT_*` overrides and installing
controlled no-global/no-system/no-prompt Git configuration. Non-Git provider auth,
HOME and PATH are preserved without mutating the parent environment.

Two focused Python regressions cover the poisoned repository and both child-spawn
APIs. The primary separately ran the actual Rust runtime plus harness and a local
fake provider with poisoned Git routing/config/hooks. It completed the isolated
fixture while preserving the unrelated HEAD, index and config byte-for-byte;
the injected hook did not run. A non-secret auth sentinel also reached the fake
worker, demonstrating preserved provider environment through the owned supervisor.
No provider API was called by this regression. These are operator-harness changes;
the previously verified Rust tree is unchanged.


The single Astra Max package-closing review passed after the targeted Git-environment
fix and recheck, with no open blockers. Its qualified scope and evidence are
recorded in [M2C-REVIEW.md](M2C-REVIEW.md).


## Milestone 3A desktop preview (2026-10-29)

- Integrated Rust workspace: **96 tests passed**, including seven new bridge
  regressions. Formatting and workspace Clippy passed with warnings denied.
- Actual supervisor regression verifies demo start, pending question, answer,
  completion and cancellation. Repeated observer reads leave follower delivery,
  acknowledgement and wake-up state unchanged. Dropping the observer preserves
  the externally attached supervisor.
- Python operator/desktop launcher regressions: **13 passed**. The actual core
  binary also passed launcher readiness/teardown with a disposable window fixture.
- Frontend typecheck/build and **14 focused state regressions passed**. CUA checked
  390x840 and 320x500 viewports, five destinations, persisted appearance, scoped
  task details, explicit missing effort, and the normal browser's disconnected
  state. Browser fixture data is enabled only with explicit DEV `?preview=1`.
- `cargo tauri build --debug --no-bundle` produced a standalone executable with
  embedded frontend assets and a separate native Cargo lock. Native Clippy passed.
- The first NVIDIA/Wayland launch exited with protocol error 71. The process-local
  `__NV_DISABLE_EXPLICIT_SYNC=1` retry created a native Wayland window
  (`xwayland: false`); the user confirmed correct visible content.
- The preview performs no paid model calls and does not install OS services,
  alter global graphics settings, consume native follower events, or modify the
  user's NixOS configuration. Native macOS/Windows execution remains unverified.

The package-closing review found two focused UI corrections: chat selection during
pending mutations could strand the action state; absent provider telemetry could
be labeled as recorded. Their targeted fixes and rechecks are tracked with the
M3 checkpoint rather than repeating a full package review.

The initial native user test produced two actual demo runs, both with a recorded
`Continue` answer and `completed` state. The user did not recognize the outcome
because completed rows were collapsed. The follow-up presentation fix routes demo
actions to Activity and opens Finished; a regression and browser UI check confirm
the completed row remains visible. The updated standalone window was reopened on
native Wayland. This is GUI-to-local-runtime evidence, not native Codex orchestration.

## M5B reviewed dependency inputs (2026-10-31)

The receipt-bound dependency-input extension adds explicit fan-in and chained
headless task execution without changing public dispatch arguments. Its operator
sequence is documented in [dependency inputs](DEPENDENCIES.md).

Accepted frozen-source gates:

- Rust workspace: **307 tests passed**, no failures.
- Standalone desktop Rust workspace: **23 tests passed**, no failures.
- Frontend: **140 tests passed**; TypeScript and production Vite build passed.
- Workspace and standalone desktop formatting and all-target Clippy passed with
  warnings denied. Both Cargo lockfiles retain their package versions; changes
  add only the local operations dependency edges.

Behavioral evidence uses real disposable Git repositories and deterministic fake
providers. It covers A+B -> C -> D, exact reviewed parent code in the child,
unchanged primary checkout, immutable/idempotent preparation, merge conflicts,
interrupted preparation, missing/replaced proofs, raw-byte seal changes after
queue and before admission, exact frozen instruction/profile/worktree identity,
and overlapping direct/transitive predecessor selection. An internally valid
replacement seal, journal and ref cannot relabel a recorded run. Started children
retain cancellation/activity access after parent changes; stale cancellation is
rejected and restart leaves in-flight work Unknown without replay.

The single Astra Max package-closing review found five required corrections:
pending-intent binding before effects, complete predecessor identity, exact
selection on memo hits, no-dereference symbolic-ref protection, and static private
JSON errors. The targeted source recheck cleared these corrections and historical
summary pinning. Raw review/generation/runtime logs remain private, outside the
repository. Review does not substitute for actual test and package evidence.

Parallel harness runs on this host intermittently returned ETXTBSY while starting
freshly written fake-provider scripts in the unchanged worker foundation suite.
The isolated pipe-holder test and all 24 foundation tests passed with a serial
harness. No assertions or runtime launch policy were weakened. The final full
Rust gate uses `--test-threads=1`; tests still exercise their own concurrent tasks,
queueing, cancellation and race scenarios. Default-parallel harness stability is
not claimed by that result; the precise environmental cause remains unproven.

These are Linux/NixOS fixture gates, not native macOS/Windows acceptance. Live
native onboarding, production multi-chat delivery, interactive provider steering,
and real provider model/effort telemetry remain separate gates. The earlier Go UI
generation attempt timed out; its draft was manually reviewed and tested, not
promoted to a successful provider run or automatically retried.
