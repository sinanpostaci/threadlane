# Codex MCP Startup Setup (`threadlane-codex-setup`)

Threadlane provides a focused Rust helper (`threadlane_native::codex_setup`), a standalone CLI (`threadlane-codex-setup`), and a desktop startup integration that register the **unbound paired** `mcp_servers.threadlane` entry in `${CODEX_HOME:-$HOME/.codex}/config.toml`.

## Boundary and Safety Invariants

- **Direct surgical TOML edit**: Reads and edits only `mcp_servers.threadlane` in `${CODEX_HOME:-$HOME/.codex}/config.toml` using `toml_edit` (`0.25.17`). All other settings, tables, and comments are preserved. No `codex` subprocess is spawned to register the server, and no raw TOML or secret values are echoed to logs, receipts, or UI surfaces.
- **Unbound paired MCP server only**: Registers only:
  - `command = "<canonical absolute threadlane-native-mcp path>"`
  - `args = ["--state-dir", "<DIR>", "--pairing-dir", "<DIR>", "--registry-dir", "<DIR>", "--plans-dir", "<DIR>", "--authority-dir", "<DIR>", "--sessions-dir", "<DIR>"]`
  - Never writes `--binding`, `--thread`, `--auto-start-approved-hosts`, `--host-executable`, notification flags, or `env`/credential overrides.
- **Registration is not enrollment**: Every status report sets `pairing_required: true`. Registering `mcp_servers.threadlane` in `config.toml` never claims an active MCP connection, an enrolled conversation, a verified native thread UUID, an operator plan approval, or a model execution run.

## Ownership Receipt, Upgrade, and Conflict Rules

Threadlane stores a small private (`0600`) ownership receipt at `<management-dir>/codex-setup-receipt.json` guarded by a non-blocking exclusive lock (`try_lock_exclusive`) on `<management-dir>/codex-setup.lock`:

1. **Absent `mcp_servers.threadlane`**: Creates `[mcp_servers.threadlane]` atomically (`0600` temp file -> `fsync` -> recheck original `config.toml` inode/mtime/SHA-256 -> atomic rename -> directory sync), records the SHA-256 digest of the canonical managed table in `codex-setup-receipt.json`, and returns `state: "registered"` with `restart_required: true`.
2. **Exact matching table**: Returns `state: "unchanged"` (`registered: true`, `config_modified: false`, `restart_required: false`) without touching `config.toml` or its modification time. If a prior run renamed `config.toml` before writing the receipt, `apply` adopts the exact matching table into `codex-setup-receipt.json` without rewriting `config.toml`.
3. **Post-commit receipt or sync warning**: If `config.toml` is renamed successfully and a subsequent directory sync or receipt write fails, the helper preserves a truthful post-commit report (`state: "registered"`, `registered: true`, `config_modified: true`, `restart_required: true`, `pairing_required: true`, `error_code: "receipt_persistence_failed"` or `"post_commit_sync_failed"`) rather than claiming `config.toml` was untouched.
4. **Disabled table (`enabled = false`)**: Returns `state: "disabled"` (`registered: false`, `config_modified: false`) and leaves `config.toml` untouched, even if the entry was previously managed by Threadlane.
5. **App / store path update on an untouched owned table**: If `mcp_servers.threadlane` differs from the current desired binary or store paths, Threadlane updates it in place **only** when a valid receipt exists whose `managed_table_sha256` matches the current two-key (`command`, `args`) table in `config.toml`.
6. **Unmanaged or manually edited table**: Any unmanaged entry, edited `command`/`args` that no longer match the receipt digest, or table with manual/unknown keys (`env`, `enabled = true`, `cwd`, timeouts, etc.) returns `state: "conflict"` (`error_code: "manual_conflict"`) and is never overwritten.
7. **Lock contention, corrupt, read-only, symlink, or concurrent edits**: Non-blocking lock contention (`error_code: "setup_busy"`), malformed TOML, corrupt receipts, symlinks anywhere along `codex_home`/`config.toml`/store/management paths, read-only files or directories, overlapping store directories, or concurrent external edits return `state: "error"` (`config_modified: false`) with a fixed English message and no forced repair.

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
