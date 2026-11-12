# Codex Existing-Instance Chat Navigation

## Scope and authority boundary

`desktop_open_codex_chat` runs only from the primary `main` Threadlane window and activates an
already-running, verified local Codex desktop instance. It never launches a new Codex process,
invokes `xdg-open` or any OS URL opener, executes shell commands, or accepts raw URLs, paths, or
argv from the frontend (`OpenCodexChatRequest` enforces `deny_unknown_fields`). Transport `ACK` and
window focus never record operator approval, worker dispatch, message delivery, viewing, or reading.

## Target resolution

Requests supply `(source_id, thread_uuid, optional job_id)` with a canonical lowercase hyphenated
non-nil UUID and bounded correlation IDs (`<= 128` bytes). Resolution queries read-only
`ObservationService::workspace_snapshot` (`limit = 100`, plus at most one targeted `job_id` read
when job histories are capped) and requires a unique verified four-part `WorkspaceChatIdentity`
(`source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`). Verified jobless chats
(`job_id: None`) are supported; when `job_id` is supplied, the job must match the exact same chat
identity. Source-only legacy records, prose mentions, and ambiguous bindings fail closed.

## Qualified Linux Owl transport and compositor focus

1. **Profile precedence**: When `XDG_CONFIG_HOME` is set, `$XDG_CONFIG_HOME/Codex` is authoritative
   and missing or invalid values fail closed without falling back. `$HOME/.config/Codex` is used
   only when `XDG_CONFIG_HOME` is unset. Multiple candidate profiles return `codex_profile_ambiguous`.
2. **Runtime and peer identity**: `SingletonLock`, `SingletonCookie`, and `SingletonSocket` (`<= 100`
   bytes inside a `0700` same-UID directory) must match in constant time. Kernel `SO_PEERCRED` and
   `/proc` (`boot_id`, `stat` `start_ticks`, `exe`, `cmdline`) are verified before write and after
   `ACK` against a root-owned, non-writable Owl `157.0.8103.39` (`schemaVersion: 1`, `linux`, `x64`)
   runtime whose `app.asar` header identifies `openai-codex-electron` with no `--app` override.
3. **Wire frame**: Sends one bounded (`<= 4 KiB`) frame
   `START\0<canonicalProjectScope>\0<canonicalExe>\0codex://threads/<uuid>` followed by write-EOF
   (three NUL separators, no `argc`/extra fields, no trailing NUL) and requires exact `ACK` within a
   `3,000 ms` total / `750 ms` per-step timeout budget.
4. **Same-window focus verification**: Before connecting to `SingletonSocket`, Threadlane authenticates
   the supported Wayland compositor socket (`< 1 MiB` response cap, same-UID `SO_PEERCRED`, distinct
   compositor PID, `/proc` continuity; non-dumpable wrapped Hyprland `EACCES` on `/proc/<pid>/exe`
   is accepted only with validated UID, `stat`, and `comm`). Hyprland `j/clients` requires a unique
   mapped, non-hidden target-PID hex address, then after `ACK` runs `dispatch focuswindow address:<hex>`
   and confirms `j/activewindow`. Sway `GET_TREE(4)` requires a unique target-PID `con_id`, then
   after `ACK` runs `RUN_COMMAND(0)` `[con_id=<N>] focus` and confirms `focused == true` via `GET_TREE(4)`.

## Coalescing, fail-closed errors, and `Unknown` latch

Pre-send rejections (`codex_app_unavailable`, `codex_runtime_unsupported`,
`codex_navigation_unsupported`, `codex_profile_ambiguous`, `codex_window_ambiguous`,
`codex_focus_unavailable`) strip local paths, cookies, and window identifiers before returning.
Concurrent in-flight (`64`) and rapid post-submit (`750 ms`, `128`) duplicates coalesce per
`WorkspaceChatIdentity`. Once socket write begins, any timeout, bad `ACK`, PID/compositor drift, or
unconfirmed focus latches `codex_activation_unknown` (`512`, non-evicting) and blocks replay.

## Evidence and platform status

Unit tests and owned separate-process Linux socket/compositor fixtures verify framing, TOCTOU
aborts, focus confirmation, and non-replay. Isolated custom-ASAR Owl `157.0.8103.39` NixOS VMs
exercised singleton framing plus Sway and Hyprland focus as runtime fixture checks only; real Codex
application thread navigation and final new desktop VM acceptance remain pending. Linux (`Hyprland`
and `Sway`) is first; other Linux sessions fail closed, macOS is not claimed, and Windows is deferred.
