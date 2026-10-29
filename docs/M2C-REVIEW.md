# M2C package-closing review

Date: 2026-10-29. Reviewer: GPT-6 Astra, effort max, as requested by the user.
Implementation: AGY Gemini 4 Argon High; ordinary review and validation by the
primary agent. One consolidated closing review was followed by a targeted
recheck of its finding, without repeating the full Rust review.

## Disposition

Qualified pass at candidate `1a227998898bf63626156a782a12fbf1e81d65e6`.
No open blocking findings remain for scoped provider file permissions, the
isolated operator harness and the qualified single-chat native worker pilot.

The closing reviewer found one P1 issue: inherited Git repository/configuration
variables could redirect the operator fixture into an unrelated repository and
commit its staged changes. The targeted correction is commit `72751da`:
Git, Threadlane CLI and owned supervisor children now share a sanitized Git
environment, while preserving non-Git provider authentication and the parent
environment. Two focused regressions and the reviewer's original disposable
reproduction passed after the correction. The primary also verified actual
runtime-to-worker environment propagation with a local fake provider and a
non-secret authentication sentinel. The unrelated HEAD, index and configuration
were unchanged, and the injected hook did not run.

## Evidence reviewed

- Primary validation: 89 Rust tests with the serial fixture harness, formatting,
  Clippy with warnings denied, locked build and deterministic CLI smoke passed.
- Seven Python regressions passed, together with actual-runtime/fake-provider
  success, missing-effort and poisoned-environment probes. The reviewer reran the
  two focused Git-environment regressions and its original reproduction.
- An actual prior-binary upgrade probe preserved the legacy Inherit profile
  digest and original idempotent snapshot without launching a duplicate process.
- One real native MCP start/get flow produced the expected Argon edit in an owned
  task worktree and preserved source state. A fixed wake-up returned to the same
  authorized native chat for event pull/ACK and terminal inspection. Missing
  observed effort correctly retained Failed/ExecutionFailed; the patch was not
  accepted or merged.
- A separate deterministic local process verified native revision-checked cancel
  and terminal event delivery. This was not another provider API invocation or
  proof of cancelling a real provider session.

Raw provider logs, chat records, binding IDs and private fixture repositories are
not part of this commit. Owned temporary test configuration/processes were
removed. Dirty fixture worktrees remain private at their original paths for
inspection; they were not force-cleaned.

## Remaining boundaries

This checkpoint does not establish patch acceptance, authenticated native
multi-chat routing, busy-turn delivery, interactive provider Answer/Steer,
live OpenCode/Go execution, reviewed result integration, the desktop UI or
cross-platform acceptance. Provider file grants are not an OS sandbox, and
validation does not recursively screen every descendant symlink. The existing
default parallel fixture-harness limitation is unchanged.

See [validation evidence](VALIDATION.md), [worker permissions and operator usage](WORKERS.md)
and [native routing gates](NATIVE-ROUTING.md).
