# Codex startup setup

Threadlane provides a focused Rust helper (`threadlane_native::codex_setup`), a standalone CLI (`threadlane-codex-setup`), and a desktop startup integration that register the **unbound paired** `mcp_servers.threadlane` entry in `${CODEX_HOME:-$HOME/.codex}/config.toml`. The desktop also installs or updates its bundled user-level `threadlane` skill independently of MCP registration. The diagnostic `threadlane-skill-setup` CLI performs only skill setup.

## Boundary and Safety Invariants

- **Direct surgical TOML edit**: Reads and edits only `mcp_servers.threadlane` in `${CODEX_HOME:-$HOME/.codex}/config.toml` using `toml_edit` (`0.25.17`). All other settings, tables, and comments are preserved. No `codex` subprocess is spawned to register the server, and no raw TOML or secret values are echoed to logs, receipts, or UI surfaces.
- **Standard owned managed paired server only**: Registers only:
  - `command = "<canonical absolute threadlane-native-mcp path>"`
  - `args = ["--state-dir", "<DIR>", "--pairing-dir", "<DIR>", "--registry-dir", "<DIR>", "--plans-dir", "<DIR>", "--authority-dir", "<DIR>", "--sessions-dir", "<DIR>", "--coworker-native-notices", "--auto-start-approved-hosts"]`
  - The two trailing booleans are the managed desktop opt-in for fresh-only approved-host startup and verified managed native notices. They are not authority, binding, model, executable, or notification-target overrides. Raw `NativeMcpCli` and `PairedMcpServer` defaults remain off for legacy and manual callers.
  - Never writes `--binding`, `--thread`, `--host-executable`, arbitrary notification flags/paths, or `env`/credential overrides. MCP arguments cannot select an executable, thread, socket, model or effort.
- **Registration is not enrollment**: Every status report sets `pairing_required: true`. Registering `mcp_servers.threadlane` in `config.toml` never claims an active MCP connection, an enrolled conversation, a verified native thread UUID, an operator plan approval, or a model execution run.

## Ownership Receipt, Upgrade, and Conflict Rules

Threadlane stores a small private (`0600`) ownership receipt at `<management-dir>/codex-setup-receipt.json` guarded by a non-blocking exclusive lock (`try_lock_exclusive`) on `<management-dir>/codex-setup.lock`:

1. **Absent `mcp_servers.threadlane`**: Creates `[mcp_servers.threadlane]` atomically (`0600` temp file -> `fsync` -> recheck original `config.toml` inode/mtime/SHA-256 -> atomic rename -> directory sync), records the SHA-256 digest of the canonical managed table in `codex-setup-receipt.json`, and returns `state: "registered"` with `restart_required: true`.
2. **Exact matching table**: Returns `state: "unchanged"` (`registered: true`, `config_modified: false`, `restart_required: false`) without touching `config.toml` or its modification time. An optional boolean `enabled = true` is preserved. If a prior run renamed `config.toml` before writing the receipt, `apply` adopts the exact matching table into `codex-setup-receipt.json` without rewriting `config.toml`.
3. **Post-commit receipt or sync warning**: If `config.toml` is renamed successfully and a subsequent directory sync or receipt write fails, the helper preserves a truthful post-commit report (`state: "registered"`, `registered: true`, `config_modified: true`, `restart_required: true`, `pairing_required: true`, `error_code: "receipt_persistence_failed"` or `"post_commit_sync_failed"`) rather than claiming `config.toml` was untouched.
4. **Disabled table (`enabled = false`)**: Returns `state: "disabled"` (`registered: false`, `config_modified: false`) and leaves `config.toml` untouched, even if the entry was previously managed by Threadlane.
5. **Receipt-verified managed upgrade**: If `mcp_servers.threadlane` differs from the current desired binary, store paths or managed flags, Threadlane updates it in place **only** when a valid receipt exists whose `managed_table_sha256` matches the current canonical `command` and `args` in `config.toml`. The recognized historical shapes are the legacy 12-element store-only vector and the 13-element vector ending in `--coworker-native-notices`; the current 14-element vector adds `--auto-start-approved-hosts` after that exact prefix. An optional boolean `enabled = true` and its comments are preserved; it is excluded from the digest so existing ownership receipts remain valid. A missing or mismatched receipt never permits an upgrade.
6. **Unmanaged or manually edited table**: Any unmanaged entry, edited `command`/`args` that no longer match the receipt digest, non-boolean `enabled`, table with manual/unknown keys (`env`, `cwd`, timeouts, etc.), or unrecognized argument shape (including auto-start without managed notices, reordered flags, values, or extra options) returns `state: "conflict"` (`error_code: "manual_conflict"`) and is never overwritten.
7. **Lock contention, corrupt, read-only, symlink, or concurrent edits**: Non-blocking lock contention (`error_code: "setup_busy"`), malformed TOML, corrupt receipts, symlinks anywhere along `codex_home`/`config.toml`/store/management paths, read-only files or directories, overlapping store directories, or concurrent external edits return `state: "error"` (`config_modified: false`) with a fixed English message and no forced repair.

### Managed startup behavior and restart boundary

The standard owned registration combines fresh-only approved-host auto-start with the existing
managed native-notices opt-in. When both are enabled on a verified managed Linux desktop session,
an eligible initial `start_approved_task` may start one fresh host and attach a fixed native
notification back to the exact enrolled chat, independently verified parent endpoint and frozen
approval repository/thread. A notice only directs the Coworker to read actual approved task/event
state; it is not approval, acceptance, human reading, or permission to dispatch, commit, merge or
run another role.

The notification context is created only for an eligible fresh launch, before process spawn, from
the verified scope. Already-running hosts keep their recorded policy. Stopped, failed, orphaned,
unknown, superseded or non-fresh sessions are never resurrected by this path. MCP JSON cannot supply
socket, executable, model, effort or notification overrides.

Upgrading an older owned registration changes `config.toml`, so `restart_required: true` means Codex
must restart or refresh its MCP connection before the new flags take effect. The maintenance helper
does not edit configuration outside the selected `CODEX_HOME`, does not enroll a chat, and does not
start a host. Fixture coverage does not establish live native-chat delivery;
installed desktop auto-start, restart recovery, busy-turn delivery and general multi-chat routing
remain separate acceptance gates.

## CLI Usage (`threadlane-codex-setup`)

```bash
threadlane-codex-setup \
  [--check] \
  [--codex-home <DIR>] \
  [--mcp-executable <ABS>] \
  --state-dir <DIR> \
  --pairing-dir <DIR> \
  --registry-dir <DIR> \
  --plans-dir <DIR> \
  --authority-dir <DIR> \
  --sessions-dir <DIR> \
  --management-dir <DIR>
```

### Flags

- `--check`: Inspect in read-only mode (`mode: "check"`). Never creates or modifies directories, lock files, receipts, or `config.toml`.
- `--codex-home <DIR>`: Optional explicit Codex home directory. When omitted, resolves `CODEX_HOME`, then `$HOME/.codex`.
- `--mcp-executable <ABS>` (alias `--mcp-binary <ABS>`): Optional explicit path to `threadlane-native-mcp`. When omitted, resolves `threadlane-native-mcp` from `PATH`, then falls back to the canonical sibling of the running binary.
- `--state-dir <DIR>`: Threadlane runtime supervisor state directory.
- `--pairing-dir <DIR>`: Private (`0700`) native MCP pairing directory.
- `--registry-dir <DIR>`: Workflow registry directory (`registry.json`).
- `--plans-dir <DIR>`: Workflow plans directory (`plans.json`).
- `--authority-dir <DIR>`: Operator approval authority directory (`approvals.json`).
- `--sessions-dir <DIR>`: Approved host sessions root directory.
- `--management-dir <DIR>`: Private (`0700`) setup management directory (`codex-setup.lock` and `codex-setup-receipt.json`).

### JSON Output (`CodexSetupReport`)

```json
{
  "state": "registered",
  "mode": "apply",
  "config_path": "/home/operator/.codex/config.toml",
  "registered": true,
  "config_modified": true,
  "restart_required": true,
  "pairing_required": true,
  "error_code": null,
  "message": "Registered mcp_servers.threadlane in Codex config.toml. Restart or refresh Codex MCP connections to load the updated configuration; conversation pairing is still required."
}
```

## Desktop Startup Integration

- On startup, `threadlane-desktop` spawns a one-time background `apply` task using the active `DesktopControlState` paths (`registry_dir`, `plans_dir`, `authority_dir`, `sessions_dir`), `app_data_dir/pairing`, `app_data_dir/codex-setup`, `CODEX_HOME` (or `$HOME/.codex`), and the runtime `--state-dir` (or original `THREADLANE_CLI_XDG_STATE_HOME` / `XDG_STATE_HOME`, then `$HOME/.local/state/threadlane/runtime`).
- The frontend launches `desktop_codex_setup_status` once independently during startup initialization (without blocking normal state polling and never inside the recurring activity poll) and renders a compact status card in **Settings > Connections** with explicit **Recheck** (`desktop_codex_setup_recheck`) and **Retry setup** (`desktop_codex_setup_apply`) buttons.
- Browser preview mode (`?preview=1` or non-Tauri browser context) never executes setup and never simulates registration success.

## Bundled Threadlane skill

The desktop startup task also checks the two embedded skill resources and
installs or updates them in `$HOME/.agents/skills/threadlane`. `USERPROFILE` is
the fallback when `HOME` is absent. This location is independent of `CODEX_HOME`,
Threadlane runtime/registry paths, and the application installation directory.
The package embeds [SKILL.md](../skills/threadlane/SKILL.md) and
[Codex metadata](../skills/threadlane/agents/openai.yaml); startup needs no network
request, source checkout, provider call, extra backend, or persistent service.

Start a new Codex chat with `$threadlane` and describe the work naturally. The
only optional skill input is a profile name or ID, for example:

```text
$threadlane profile=default
Help me investigate the model count shown in Tools.
```

This is a conversational instruction, not a shell command or typed MCP argument.
Without an option, the Coworker resolves the configured default profile from the
live catalog. A named profile applies to the next scoped plan/approval; it does
not save global settings or replace an active frozen execution profile. Missing
or ambiguous selections need clarification. There is no mode switch: the
Coworker infers exploration, planning, status, and continuation from the request.

The skill bootstraps automatic chat connection, the shared operating guide,
profile and durable-state inspection, requirements collaboration, and the
supported Orchestrator/Architect handoffs. Architect consultation still requires
a valid planning grant and its role/model/budget allowance. A skill is guidance;
it is not model permission, execution approval, active MCP pairing, or a guarantee
of uninterrupted model memory. After context uncertainty, the Coworker refreshes
the guide and live state instead of assuming earlier authority.

Codex supports automatic discovery of user skills and their updates. If the
installed skill is not visible in a particular Codex session, check that
`SKILL.md` exists at the reported path and that Codex has not explicitly disabled
it, then use Codex's supported refresh/restart fallback. Threadlane does not
restart Codex, alter its active model, or re-enable a disabled skill. See
[OpenAI's skill documentation](https://learn.chatgpt.com/docs/build-skills).

### Ownership and recovery

- The installer manages only `SKILL.md`, `agents/openai.yaml`, and its private
  `.threadlane-managed.json` ownership journal. Unrelated skills, extra files,
  existing parent directory modes, and Codex configuration are preserved.
- An absent/empty target can be installed. Exact matching payloads can be adopted
  without rewriting them. Other unmanaged files or edits to owned resources
  produce a conflict and are preserved; Retry setup never forces replacement.
- A shared nonblocking lock in the user skills root serializes setup across
  Threadlane runtimes. Repeat setup of unchanged resources does not rewrite them.
- Updates journal the previous and desired hashes before replacing resources and
  finalize only after durable writes. A later launch can recover known partial
  updates. Unknown edits, malformed ownership data, symlinks, nonregular paths,
  and read-only resources are rejected. A post-write error is reported as such,
  including whether files changed, rather than reported as success.
- These filesystem checks do not claim Windows support or physical power-loss
  guarantees. Live discovery and managed conversation acceptance are separate
  from structural skill validation and local filesystem tests.

### Skill diagnostic CLI and desktop status

```sh
threadlane-skill-setup --check
threadlane-skill-setup
threadlane-skill-setup --skills-dir /absolute/test-home/.agents/skills --check
```

`--check` creates nothing. The default Apply installs or refreshes only the owned
skill resources. Reports distinguish `installed`, `updated`, `unchanged`,
`not_installed`, `outdated`, `conflict`, and `error`; conflicts and errors exit
nonzero. `installed: true` means the payload matches the evaluated bundle, not
that Codex has activated it or enrolled the current chat.

Settings → Connections shows MCP registration and **Threadlane skill** separately.
The existing **Recheck** and **Retry setup** actions inspect or apply both. Desktop
IPC preserves the original MCP report fields and adds optional `skill_setup`;
legacy reports without it mean unknown skill state. Browser preview performs no
installation. The original `threadlane-codex-setup` CLI remains MCP-only.
