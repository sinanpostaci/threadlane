# Desktop Control Panel (Milestone 3A)

## Current desktop workspace

The production entry point (`apps/desktop/index.html` → `apps/desktop/src/main.ts`
→ `apps/desktop/src/app.tsx`) uses React and Radix Themes while preserving the
`?surface=tray` route (`apps/desktop/src/tray.tsx`) for the compact tray popover.
`DesktopAppCoordinator` loads a single bounded `WorkspaceSnapshot` via
`desktop_workspace_snapshot` (`ipcWorkspaceSnapshot`) and projects chat-first
workspace state through `projectWorkspaceSnapshot`. Interface chrome is compact,
neutral, and English; observed chat titles, job titles, summaries, questions, and
results are rendered verbatim in their original language without translating UI
controls.

`Projects` provides two explicit navigation submenus—**Active** (`Projects/Active`)
and **Archive** (`Projects/Archive`)—both in the desktop sidebar and in the
responsive drawer. Each chat row is keyed by its verified four-part
`WorkspaceChatIdentity` (`source_id`, `binding_id`, `thread_uuid`,
`canonical_project_scope`) and offers per-chat **Archive** and **Restore** actions
persisted locally (`threadlane.desktop.workspace.chatArchive.v1`). Archiving is a
local visibility preference that never deletes backend records; storage write
failures surface as explicit alerts, and active human blockers inside
`Projects/Archive` remain surfaced in `Projects/Active` so blocked conversations
are never silently buried. Legacy `6h` / `24h` time-window toggles, `Older (6h+)`
badges, `Load older activity` paging controls, bulk `Archive history`, and `Show
archived` cutoff buttons are removed from production `Projects`; bounded snapshot
omissions remain explicitly visible in the UI.

Selecting a chat derives the raw `WorkspaceChatSummary` and matching
`WorkspaceNativeSample` strictly by full four-part canonical chat identity
equality, with no source-only fallback or cross-chat data spill, and mounts
`WorkspaceChatPanels` with three separate tabs:

- **Messages** (default): bounded, read-only timeline of safe recorded summaries,
  questions, answers, review verdicts, and job results, with automatic bottom
  following, reading-position preservation on upward scroll, and a floating
  jump-to-latest indicator when new messages arrive. No raw provider transcript is
  read and no message composer is offered.
- **Jobs**: durable per-chat job, task, attempt, candidate, and backend-verified
  acceptance-proof ledger.
- **Activity**: chronological request, child-call, and result-delivery audit feed
  with centered live `ActivitySpinner` alignment and cross-tab job filtering.

Live Coworker state and verified configured model selection are displayed
independently of Worker execution telemetry; stale or disconnected native samples
withhold live claims rather than guessing. Neither a finished Worker run nor any
completed MCP tool call ever marks a conversation or job `Completed`; authoritative
completion requires backend-verified acceptance proof.

The sidebar toggle button resides exclusively in the custom titlebar: on wide
windows (`> 760px`) it collapses and expands the sidebar, and on narrow windows
(`<= 760px`) it opens and closes an accessible Radix `Dialog` modal navigation
drawer with keyboard focus restoration and `Escape` dismissal. The minimum native
window is 360 × 500 px and the initial window is 1040 × 760 px. `Tools` retains
its catalog/model-family editor; `Profiles` retains the saved profile list, five
roles, four difficulty overrides, explicit reset controls, and instance-preserved
dirty drafts across section changes; `Settings` retains appearance, window close
behavior, explicit Codex setup controls (never auto-written on UI startup), and
linked observation sources. Saving configuration grants no planning or execution
permission.

When a system tray is available, closing the main window hides it by default.
Settings → Window close behavior can select Quit instead. The tray offers Open
and Quit, and its small popover displays a bounded observation summary. Quit is
required to fully restart a hidden app after an update. If tray creation fails,
the window remains the available control surface. An isolated test launch may use
`--no-codex-startup-setup` to avoid modifying Codex setup; it must use separate
XDG/runtime directories.

Open in Codex submits only a canonical `codex://threads/<uuid>` URL through
`desktop_open_codex_chat`. The Rust backend independently revalidates the workspace
chat identity (`source_id`, `thread_uuid`, and optional `job_id`) against bounded
read-only observation data before invoking the OS opener. Unsupported environments
return an explicit error with no arbitrary URL, launcher, or shell fallback. A
successful submission does not prove that Codex displayed the chat or that the
human read or approved anything. System notifications are separate future work.

Linux is the current qualification target. macOS has shared source preparation
only; native macOS tray/navigation and packaging are unverified. Windows is
deferred. Browser fixtures exercise the real React components through an isolated
IPC stub; integrated combined-checkout checks, wide/narrow Playwright runs on the
merged production bundle, isolated NixOS Wayland VM tray and Codex routing
acceptance, authorized live workflow qualification, and the single `gpt-6-astra`
(`high`) package-closing review remain separate gates tracked in
[validation](VALIDATION.md) and [MVP readiness](MVP-READINESS.md).

For the current [Projects view](PROJECTS-VIEW.md) contract, read-only observation
semantics, and explicit Codex setup boundaries, also read
[desktop observation](OBSERVATION.md) and [Codex MCP startup setup](CODEX-SETUP.md).

The historical Milestone 3A description below predates the durable read model.
The new observation feed reads durable metadata directly in read-only mode so
stopped sessions remain visible; the legacy runtime diagnostics still use the
authenticated supervisor client. For the current
editable tools/profiles and approved local workflow, read
[configuration](CONFIGURATION.md), the [M4 checkpoint](M4-CHECKPOINT.md), and
[optional session controls](SESSION-LIFECYCLE.md).

Milestone 3A introduces the first usable Threadlane desktop control panel (`apps/desktop` and `crates/threadlane-desktop-bridge`) together with an isolated local preview launcher (`scripts/desktop_preview.py`).

## Scope and Current Limits

- **Observer + owned local simulation only**: The desktop app is a read-only observer for an attached `threadlane serve` runtime plus an explicitly labeled, locally owned deterministic demo (`Local demo`).
- **No paid workers from the UI**: Milestone 3A does not start, steer, answer, or cancel paid coding workers (`antigravity` or `opencode`) from the desktop window.
- **No live chat orchestration claims**: Native Codex remains the user's conversational interface. This milestone does not solve authenticated multi-chat routing, busy-turn delivery, interactive worker continuation, missing provider effort telemetry, reviewed Git patch integration, editable model/profile registries, or live Jev integration (`jev` is reported as `not_configured`).
- **Core and MCP remain unchanged**: No SQLite schema, `threadlane-core` state machine, or stdio MCP protocol changes are introduced. The root Cargo workspace remains free of GTK/WebKit dependencies; the Tauri shell (`apps/desktop/src-tauri`) is a standalone nested Cargo workspace that depends on `crates/threadlane-desktop-bridge` by path.
- **Platform verification**: Linux/NixOS is the verified host environment. Cross-platform Python and Nix expressions are written defensively, but macOS and Windows desktop builds remain unverified until Milestone 5.

---

## Model families and concrete CLI variants

Tools groups Antigravity's separate effort-suffixed CLI records (for example, low, medium, and high) into one model family only when both the display label and model argument identify the same base model and effort. The saved records remain separate concrete CLI pins: their exact IDs, labels, model arguments, and effort metadata are unchanged, and each one stays available under **CLI variants** on the family card. Profiles can offer one family choice whose effort resolves to those existing records. Other tools continue to show one card per model, and unrecognized or ambiguous records stay ordinary. This grouping is UI-only: it adds no migration, rewrites no saved records, and never probes or executes a CLI.

---

## Runtime, Security, and UI Invariants

1. **Sole Live Data Boundary (`SupervisorClient::from_state_dir_lazy`)**:
   - The bridge never opens `state.db` directly and never calls `pull_events` or `ack_events` (which would consume the native Codex follower's event/wake-up cursor state).
   - Every poll and detail request re-validates the private state directory (`0700`), `supervisor.json` (`0600`), non-symlink path components, loopback host (`127.0.0.1`), and bearer token via the existing Rust client.
   - Recent run summaries (`<= 10` per binding) are enriched via `get_worker`; only `WorkerNotFound` falls back to `get_run`. Transport or descriptor security failures surface as disconnected/blocked attachment state rather than fabricated empty data. Missing model or effort evidence remains `failed` / `execution_failed`.
2. **Strict Separation of Observer and Local Demo**:
   - `--state-dir <EXISTING_RUNTIME>` (or the in-app attach control) attaches the observer to an existing runtime without creating or mutating state.
   - `--demo-state-dir <OWNED_DEMO_RUNTIME>` points at a dedicated demo supervisor. Clicking the explicit demo start action creates one synthetic `Local demo` binding and fixed demo instruction on that demo runtime and remembers the owned `(binding_id, run_id)` tuple for the process lifetime.
   - Only that owned demo tuple can be answered (fixed `"Continue"` answer) or cancelled from the UI, and mutations require a matching `expectedRevision`. The owned demo serializes its own mutations; cancellation uses a precheck because the runtime cancel API does not provide server-side revision compare-and-swap.
   - Closing the desktop window never stops an attached external supervisor or any running workers.
3. **Path Discovery & Untrusted Data Rendering**:
   - `desktop_tool_status` inspects `PATH` for executable files only (`codex`, `opencode`, `agy`, `git`) without executing provider CLIs or model APIs.
   - Every runtime label, prompt, summary, question, and path is rendered as plain text (`textContent` / DOM text nodes). Rejected IPC calls return short, safe English error strings without bearer tokens, raw descriptors, or internal backtraces.
4. **Window Geometry and Glass Surface**:
   - The window defaults to `390x840` (minimum `320x500`), resizable and transparent, without global compositor/window-manager rules.
   - Appearance settings (`System` / `Light` / `Dark`, `Glass` / `Solid` with `65%..100%` surface opacity while keeping text opaque, and `sage` / `blue` / `violet` / `sand` accents) persist only UI appearance preferences and an optional explicit observer path.
   - True background blur behind a transparent window depends on the host desktop compositor; CSS `backdrop-filter` alone only blurs elements within the webview document and does not sample the external desktop wallpaper.
   - Opening the frontend in a normal browser without Tauri reports a real disconnected state; static design fixtures require explicit Vite dev mode with `?preview=1` and are labeled as a design preview.

---

## Development Shell (Nix)

`flake.nix` keeps `devShells.<system>.default` unchanged for core Rust/CLI work and adds `devShells.<system>.desktop` with `cargo-tauri`, `nodejs`, `pkg-config`, and platform libraries:

- **Linux**: `gtk3`, `webkitgtk_4_1`, `libsoup_3`, `openssl`, `librsvg`, `glib-networking`, and `gsettings-desktop-schemas`. The shell hook prepends `glib-networking` to `GIO_EXTRA_MODULES` and prepends the `gsettings-desktop-schemas` and `gtk3` schema/data paths to `XDG_DATA_DIRS` (falling back to `/usr/local/share:/usr/share` if `XDG_DATA_DIRS` was unset), without overriding `LD_LIBRARY_PATH` or forcing global graphics workarounds.
- **Darwin**: Conditional `openssl` and `libiconv` without evaluating Linux-only GTK/WebKit packages.

### Entering the Desktop Shell

In a normal checked-out Git repository, use the `.#desktop` flake reference so Nix only tracks Git-indexed source files:

```bash
nix develop .#desktop
```

### Tiny-Shell Note (Avoiding Build-Cache Copies with `path:`)

Avoid running `nix develop path:/abs/path/to/repo#desktop` on a checkout that already contains untracked build directories (`target/`, `.local/`, or `node_modules/`): the `path:` flake input copies the entire directory tree—including gitignored build caches—into `/nix/store`.

If you are working in a linked Git worktree or environment where `nix develop .#desktop` cannot resolve the parent Git common directory directly, copy only `flake.nix` and `flake.lock` into a tiny temporary directory and invoke `nix develop` against that clean directory while keeping your shell working directory in the repository:

```bash
tiny_flake="$(mktemp -d)"
cp flake.nix flake.lock "$tiny_flake/"
nix develop "path:$tiny_flake#desktop"
```

---

## Building the Binaries

1. **Build the core `threadlane` supervisor and CLI** (from the repository root):
   ```bash
   cargo build --workspace --locked
   ```
   This produces `./target/debug/threadlane`.

2. **Install frontend dependencies and build the Vite bundle**:
   ```bash
   npm --prefix apps/desktop ci
   npm --prefix apps/desktop run build
   ```

3. **Build the standalone Tauri desktop executable** with `CARGO_TARGET_DIR` pointed at `<repo>/target/desktop`:
   ```bash
   threadlane_root="$(pwd)"
   cd apps/desktop
   CARGO_TARGET_DIR="$threadlane_root/target/desktop" cargo tauri build --debug --no-bundle
   cd "$threadlane_root"
   ```
   Start the block from the repository root. The absolute target path keeps the
   artifact at `./target/desktop/debug/threadlane-desktop`. Tauri runs the
   configured frontend build before compiling the native shell.

Standalone builds made directly with Cargo must enable the dependency feature
`--features tauri/custom-protocol` after the frontend bundle exists. The Nix
package and `scripts/build-desktop.py` set this automatically for both debug and
release builds. Without it, Tauri treats the executable as a development client
and tries `http://127.0.0.1:1420` instead of its embedded assets. A packaged
standalone window requires no local frontend server; `tauri dev` keeps its normal
development-server behavior.

---

## Running the Desktop Preview Launcher (`scripts/desktop_preview.py`)

`scripts/desktop_preview.py` coordinates an isolated local demo supervisor and the built desktop executable using Python's standard library (`subprocess` argv lists, no shell execution, and sanitized child Git environments via `live_worker_pilot.build_sanitized_child_env`):

1. **Dry-run plan (no filesystem or process side effects)**:
   ```bash
   python3 scripts/desktop_preview.py --dry-run
   ```

2. **Launch with an owned local demo supervisor**:
   ```bash
   python3 scripts/desktop_preview.py
   ```
   By default this:
   - Verifies `./target/debug/threadlane` and `./target/desktop/debug/threadlane-desktop`.
   - Creates a fresh private (`0700`) session directory at `.local/desktop-previews/<uuid>/` with `state/` and `logs/` subdirectories and `0600` log files.
   - Starts `threadlane serve --state-dir <session>/state` with Codex wake-ups and worker profiles disabled.
   - Polls `threadlane list-bindings --state-dir <session>/state` until the demo supervisor is ready (without printing `supervisor.json` or bearer tokens).
   - Launches `threadlane-desktop --demo-state-dir <session>/state`.
   - When the GUI exits, stops and reaps **only** the owned demo supervisor and writes `<session>/preview_summary.json`.

3. **Launch with an owned demo supervisor AND attach an existing external runtime for observation**:
   ```bash
   python3 scripts/desktop_preview.py --state-dir /abs/path/to/existing/runtime/state
   ```
   The attached `--state-dir` is never modified by the launcher, and its supervisor is never stopped when the desktop window closes.

4. **Run the offline launcher lifecycle tests**:
   ```bash
   PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_desktop_preview.py -v
   ```


## NVIDIA + Wayland preview on the first Linux test host

The first standalone launch exited with Wayland protocol error 71. A process-local
NVIDIA explicit-sync workaround allowed the same binary to remain open on native
Wayland; Hyprland reported `xwayland: false`, and the user confirmed visible content.
Use this invocation if that specific failure occurs on an NVIDIA/Wayland host:

```bash
__NV_DISABLE_EXPLICIT_SYNC=1 nix develop .#desktop --command python3 scripts/desktop_preview.py
```

The variable applies only to that launch and its children. The launcher does not
change compositor, driver, browser, or NixOS settings. Other platforms and GPU
configurations still need their own native smoke tests.
