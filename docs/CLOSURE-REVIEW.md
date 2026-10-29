# Foundation closure review

Reviewed baseline: `0e8c3ddb8a68fa6a70afe045ee55acd9fc8c98ea`.
Date: 2026-10-29. Scope: Milestone 1, not the future desktop UI or real workers.

The user requested an additional Opus 5.5 High review. The observed reviewer was
`claude-opus-5-5-high`, effort `high`, through AGY. It performed static inspection;
it did not run tests. The primary agent independently triaged the findings and
ran reproductions. Implementation corrections use two isolated Argon High
worktrees, with review and validation by the primary agent. Review agreement is
not runtime evidence, and the baseline review is not a review of the later patch.

## Finding decisions

| Finding | Primary evaluation and disposition |
| --- | --- |
| Accepted instruction can poison every subsequent demo tick | Confirmed against the baseline binary. A 4,096-byte instruction containing 2,411 U+0001 characters and 1,685 ASCII characters starts successfully, then produces a 16,420-byte question event against the 16,384-byte limit. Another binding fails too, including after restart. Fixed: timed events use compact references, and the original instruction stays in the run record. Regression and restart checks passed. |
| Failed or unknown wake can block future notification indefinitely | Confirmed from the durable state machine. Avoiding automatic replay is correct; the missing part is explicit operator recovery with claim correlation and a duplicate-risk acknowledgement for unknown outcomes. Implemented explicit claim-matched retry with durable audit, no automatic replay and no fabricated event acknowledgement; regression checks passed. |
| Run histories and stored mutation responses grow without bound | Valid design limitation for long sessions. Compact summaries alone do not bound full run responses. Bounded/paginated history, compact mutation results and result-size regression coverage are a gate before real workers, not a completed feature. |
| Arbitrary existing directories can be chmodded by state initialization | Confirmed from initialization code. Fixed: unrelated non-empty directories are refused before chmod or file creation; preservation checks passed. |
| Fatal `serve` remains alive holding ownership | Confirmed from the signal-only CLI wait. Fixed and tested: nonzero CLI exit after orderly cleanup, retaining diagnostics and uncertain wake history. Primary review also caught and corrected a watch-channel notification race before validation. |
| MCP initialization requires an already running supervisor | Confirmed lifecycle coupling. Fixed and tested: discovery works offline; every call validates the descriptor and the same MCP process recovers after supervisor start/restart. This was not proven to cause the native desktop discovery failure. |

## Native connection investigation

The public Codex `config/read` API, with configuration layers included, showed
that the disposable test project's local MCP entry was disabled because the
project was not trusted. With explicit user approval, a temporary trust entry
was added for that one test folder. The same API then reported the local entry
enabled and present in effective configuration.

An isolated installed Codex app-server, given the explicit MCP entry, successfully
listed all eight Threadlane tools through its real MCP client without starting a
model turn. This establishes client/protocol discovery compatibility; it does
not establish that an existing native desktop conversation loaded the tools.
The already open disposable chat still reported `MCP_UNAVAILABLE`. The public
local control socket was absent, so a supported client refresh is still needed.
No private Codex database or undocumented IPC was used, and no model, effort,
approval policy or sandbox setting was changed.

## Closure gate

The primary agent verified all 34 tests, formatting, Clippy with warnings denied,
the workspace build and process-based CLI smoke. The corrected foundation is
ready for a private checkpoint. Native acceptance still
requires actual MCP calls from the disposable native chat: status, event pull,
acknowledgement, revision-matched answer, completion and final acknowledgement.
A successful queue submission or an independent API client is not a substitute.
