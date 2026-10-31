# Native tool-output pilot (2026-10-31)

This is one qualified Linux/NixOS experiment in an explicitly authorized
disposable native Codex chat, not general multi-chat or platform acceptance.

## Frozen scope

The human approved one task in a separate temporary Git repository. The only
allowed write was `result.txt`, containing exactly `THREADLANE_NATIVE_PILOT_OK`
and a newline. Concurrency was one and the worker timeout was 120 seconds.
The plan and approval pinned OpenCode Go / DeepSeek 4.1 Flash / `max` without
fallback. The follower retained its existing Luna / `max` configuration.

The approval preview was prepared without worker execution. After human
confirmation, the exact subject digest was rechecked before recording the
approval. The test chat's working directory was temporarily switched to that
repository through the documented turn API; neither model nor permissions were
overridden. A trusted setup turn supplied the exact private operator context and
limited the follower to this approval's event pull and actual-batch ACK.

## Observed sequence

1. The new packaged host started with explicit `unix_tool_output` transport and
   operator CLI guidance, keeping notifications disabled on its bootstrap
   supervisor. One frozen task was dispatched once.
2. OpenCode 2.2.5 completed the real task in 14,366 ms. Its owned report contained
   matching requested and observed `opencode-go/deepseek-v4.1-flash` and `max`.
   The result was `ready_for_review`, with `accepted: false`.
3. An independent file check confirmed the exact canary bytes, only one changed
   path, no untracked paths, and the original source checkout still containing
   its unchanged initial file. No task commit or integration was created.
4. One software notice reached the same idle native chat as
   `functionCallOutput`, named `threadlane_events_available`. The native history
   contained no new user message in that notification turn. The notifier had
   already closed its own WebSocket after the accepted turn response.
5. Luna executed the scoped packaged `threadlane-context ... session pull`
   command and received 12 events with `has_more: false`. It then executed
   `session ack` with the actual returned batch ID. Both command executions
   completed with exit code zero in the public native history.
6. An independent read of the authenticated runtime confirmed
   `delivered_seq: 12`, `acked_seq: 12`, `attempt_count: 1`, wake-up `idle`, and
   zero active runs. The main operator did not pull or ACK this batch.
7. The exact owner/instance-scoped stop completed, its manifest became
   `stopped`, and the separately owned control process exited. The original
   native working directory and Luna Max settings were verified after teardown.

The follower correctly reported that completion was not acceptance. Its outer
control binding differed from the host-session event binding by design; both
were independently compared with the recorded scope. The truthful
`native_per_call_identity_verified: false` value was retained. This test does not
turn a trusted local context into native MCP caller authentication.

## What this proves and what remains

The actual worker -> host -> native tool output -> follower command -> durable
ACK chain worked on this host, in addition to the separate visually confirmed
idle and busy protocol experiments. The product pilot was verified through
public native history, owned worker evidence, file checks and durable runtime
counters; it is not a new blanket visual or cross-platform claim.

The 11 focused transport/lifecycle/host tests, relevant runtime/host/native
regressions, workspace Clippy, formatting, two desktop policy tests and CLI plus
desktop Nix builds passed for implementation commit `0020a2a`. Those tests use
fake local transports and providers; this pilot is the separate real-provider
and native-conversation evidence.

General multi-chat identity, concurrent human steering, app-closed delivery,
automatic endpoint refresh after restart, and macOS/Windows remain separate
gates. The temporary desktop bridge preserves the original backend and UI bundle
bytes, but its SIGKILL/orphan recovery is still manual. No global MCP settings,
NixOS source, persistent OS service or primary-project source changed during the
pilot. Its patch remains unaccepted and unintegrated.

## Optional Nix desktop bridge

The flake exports `lib.wrapCodexDesktop { pkgs, chatgpt }` for an explicit Linux
desktop package override. The source under `tools/native-bridge` is the same
reviewed launcher used in the controlled experiments; it starts one backend and
translates the desktop's JSONL connection to the selected Unix WebSocket.
The function does not install a package or enable notification policy by itself.

Pass the existing upstream package and matching package set. It retains upstream
versions and sources, preserves the original backend as
`codex-threadlane-original`, and does not patch the UI bundle. Unknown launch
arguments pass through without claiming a bridge is available. An upstream
update therefore still needs a compatibility check; a successful package build
does not establish native behavior for that version.

Endpoint selection stays explicit and private. A new application run may expose
a different endpoint; a stale context must fail rather than adopt another
server or fall back to fake user text. The helper is currently Linux-only and
is not a macOS signed-application installation method.

See [transport configuration](NATIVE-TOOL-OUTPUT.md) and
[the launcher and its limitations](../tools/native-bridge/README.md).
