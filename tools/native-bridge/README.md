# Optional Linux Native Desktop Bridge

This directory contains the opt-in launcher used in Threadlane's controlled
Linux desktop experiments. It wraps the bundled `resources/codex` executable
without modifying the UI bundle or the original backend bytes. Exporting this
helper does not install it or enable notifications. The launcher retains its
historical `prototype` diagnostic labels; those labels are not a readiness claim.

The [qualified live pilot](../../docs/NATIVE-TOOL-OUTPUT-PILOT-2026-10-31.md)
records one real worker-to-follower pull/ACK cycle. General multi-chat identity,
restart recovery and cross-platform acceptance remain open.

## Problem and Verified Framing Modes

Reference documentation: <https://learn.chatgpt.com/docs/app-server>

The observed native ChatGPT desktop bundle (`ChatGPT 26.1027.30824`, bundled CLI `0.166.0-alpha.5.1`) starts `resources/codex` with opaque configuration arguments both before and after `app-server`, plus `--analytics-default-enabled`. Values were not disclosed:

```text
resources/codex -c '<opaque1>' app-server --analytics-default-enabled -c '<opaque2>'
```

`codex app-server` supports two distinct framing modes and allows **at most one** `--listen` endpoint per process:

1. **Stdio JSONL (`stdio://` / default `app-server`)**: Newline-delimited JSON-RPC messages over standard input and standard output, with no listening control socket.
2. **Unix Domain WebSocket (`--listen unix://PATH`)**: RFC 6455 HTTP Upgrade over a Unix domain socket followed by one JSON-RPC message per WebSocket text frame.

### Why `app-server proxy` Is Not Used

`codex app-server proxy --sock PATH` is a raw byte relay over a Unix stream socket. It does **not** perform an HTTP/WebSocket handshake or translate between newline-delimited stdio JSONL and WebSocket frames. Using `app-server proxy` against a `unix://` WebSocket listener fails at the framing layer.

## Architecture

- **Single replacement backend with opaque argument preservation**: `launcher.py` (`plan_backend_argv`) intercepts `app-server` only when preceded by whitelisted global options (`-c VALUE`, `--config VALUE`, `--config=VALUE`, `--enable VALUE`, `--disable VALUE`, `--strict-config`) and followed by whitelisted `app-server` options (`--analytics-default-enabled`, `--strict-config`, the same `-c`/`--config`/`--config=..`/`--enable`/`--disable` forms, and at most one explicit stdio transport flag `--stdio`, `--listen stdio://`, or `--listen=stdio://`). Opaque option values are never parsed or modified, and an option value equal to `app-server` is not treated as the subcommand. `launcher.py` preserves all prefix and suffix arguments in original order, strips any explicit stdio listener flag/pair, and appends a sole Unix WebSocket listener (`--listen unix://<XDG_RUNTIME_DIR>/tl-codex-<random>/s.sock`) to spawn **one** instance of `THREADLANE_REAL_CODEX`:
  ```text
  <real-codex> -c '<opaque1>' app-server --analytics-default-enabled -c '<opaque2>' --listen unix://<XDG_RUNTIME_DIR>/tl-codex-<random>/s.sock
  ```
- **Unmodified passthrough**: Any unknown flag, missing option value, non-stdio listener, duplicate/mixed listener flag, helper subcommand or metadata flag (`proxy`, `startup-sign-out`, `--print-startup-config`, `--help`, `-h`), or non-`app-server` command is passed directly to `os.execv` byte-for-byte unchanged without importing `websockets` or creating a runtime directory.
- **Stdio JSONL <-> Unix WebSocket bridge**: `launcher.py` connects to the private Unix WebSocket using `websockets.asyncio.client.unix_connect` and forwards each stdin line (bounded to 8 MiB, strict UTF-8) as one WebSocket text frame and each incoming WebSocket text frame (rejecting binary frames and embedded newlines) as one stdout line, without parsing, rewriting, or injecting JSON-RPC payloads.
- **Private runtime metadata**: After handshake completion, `launcher.py` writes `0600` `descriptor.json` inside the `0700` per-run directory (`schema_version: 1`, `wrapper_pid`, `backend_pid`, `socket_path`, `real_codex`, `transport: "unix_websocket"`) and prints its path once to `stderr`.
- **Optional packaging (`package.nix`)**: Wraps `chatgpt.unwrapped` on Linux by renaming `resources/codex` to `resources/codex-threadlane-original` and installing a `makeWrapper` entrypoint at `resources/codex` pointing to `launcher.py` with `THREADLANE_REAL_CODEX` set. `app.asar` is not modified.

## Nix Integration

The Threadlane flake exports a function; the caller supplies its existing
upstream desktop package and the matching package set:

```nix
chatgpt = inputs.threadlane.lib.wrapCodexDesktop {
  pkgs = upstreamPkgs;
  chatgpt = upstream.chatgpt;
};
```

Keep the upstream package source and update mechanism. Pin the Threadlane flake
input as usual, and perform a compatibility check when the desktop version
changes. The controlled native experiments used desktop version `26.1027.30824`;
a successful build for another version does not verify its runtime behavior.
No runtime endpoint, conversation ID or private context belongs in the flake.

## Synthetic Transport Test Command

The unit test suite uses only a synthetic Python fake executable and local Unix sockets; it never launches the real Codex binary, model, or live threads:

```sh
python3 -m unittest discover -s . -p 'test_launcher.py' -v
```

## Known Limits

- **Opt-in activation**: Adding the flake input or calling the helper alone does not activate a system configuration, restart an application, or configure a Threadlane host. Those remain separate operator actions.
- **Qualified evidence**: `test_launcher.py` verifies framing, error handling and cleanup against synthetic fixtures only. Separate controlled experiments confirmed idle and busy delivery in one native Linux chat, followed by one real product pull/ACK pilot. They do not establish concurrent human steering, multi-chat routing or application-closed delivery.
- **Same-UID trust and per-chat binding**: The Unix socket is protected by filesystem permissions (`0700` runtime/session directory), meaning any local process running under the same UID can connect. `descriptor.json` only identifies the transport endpoint and PIDs; it is **not** proof of per-chat or per-thread identity. Safe live attachment still requires explicit operator selection and per-chat/thread binding verification.
- **Platform support**: Linux/Unix only (`XDG_RUNTIME_DIR` on `posix` required); Windows and macOS are unsupported and untested.
- **Bounded frame size**: Individual JSONL lines and WebSocket frames are bounded to 8 MiB (`max_queue = 16`, compression disabled); larger payloads fail closed without automatic retry.
- **Forced termination**: SIGTERM, SIGINT, SIGHUP and stdin EOF are handled. SIGKILL or a launcher crash cannot run cleanup and may leave an owned backend/socket. A controlled restart must inspect process ownership before proceeding; this prototype provides no automatic orphan recovery or production restart guarantee.
