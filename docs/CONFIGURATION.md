# Tools, models, and workflow profiles

The desktop configuration registry is independent of a running supervisor. Saving a
tool or profile does not restart workers, change their model, or grant access to a
repository. A new native installation starts with an empty registry.

## Configure a CLI tool

1. Open **Tools** and choose **Add and choose models** for a detected CLI, or
   configure a custom tool and save it.
2. Select models in its searchable catalog, then choose **Add selected models**.
3. Open **Profiles** and assign a model and one of its detected effort options.

Catalog checks read metadata without running a model. Codex exposes supported
reasoning levels; OpenCode v2 exposes per-model variants through its standalone
`model.list` metadata operation. Antigravity exposes concrete effort variants:
Threadlane uses an effort only when the model ID suffix and catalog label agree.
The concrete Antigravity model argument is preserved. Catalog options do not prove
runtime entitlement or execution telemetry.

For saved models imported before effort detection was available, load the catalog
and choose **Use detected effort options**. This fills missing options only for
matching model IDs, tool IDs and exact model arguments. Custom nonempty effort
lists, labels, model arguments, default-without-effort choices and assignments are
preserved. Manual overrides remain available under Advanced.

OpenCode catalogs may list the same model under separate Go and API routes. Use
**Model source** to choose **OpenCode Go** or **OpenCode API**, with counts from the
current catalog. Filtering never changes a model's execution route or imports it.
Selections outside the current view are reported explicitly. A catalog entry does
not confirm that the active subscription permits that model.

Native preview launchers that isolate Threadlane's XDG folders must preserve the
original CLI account directories. The trusted launcher records the original
`XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_CACHE_HOME` and `XDG_STATE_HOME` in fixed
`THREADLANE_CLI_XDG_*` markers before isolating the GUI. CLI probes and detached
execution hosts restore those values only for their child processes and remove
the markers there. Threadlane's parent stores remain isolated. No credentials are
copied, migrated or exposed to the frontend; missing markers preserve ordinary
inherited CLI behavior.

For example, an OpenCode Go model uses provider argument
`opencode-go/deepseek-v4.1-flash`, with `max` configured as a separate effort. The
worker combines them as `opencode-go/deepseek-v4.1-flash#max`.

## Configure API credentials

Jev and OpenAI Decisions use separate API adapters. Open their tool details and
submit a credential through **Set credential**. The native application stores it
in the operating system keychain, with no plaintext fallback. The input is cleared
after an attempt. A locked or unavailable keychain is shown as an error.

**Load API model catalog** requests metadata. **Run synthetic decision test** sends
one fixed sample owned by the Rust backend; it does not send conversation or project
content. Classification is advisory and does not approve work or select another
model. OpenAI platform credentials are separate from a Codex ChatGPT sign-in.

Browser design fixtures cannot store real credentials or establish API access.

## Create a profile

Open **Profiles**, choose **New profile**, and assign models to the six roles. Each
role can have a default and optional Easy, Medium, Hard, and Extreme overrides.
Only efforts configured for the selected model are offered; none is silently
chosen. Expand **Difficulty overrides** to edit those assignments.

Partial profiles can be saved and are labelled with their missing defaults.
Resolving an unassigned task fails explicitly. A difficulty-specific assignment
takes precedence over that role's explicit default; another model is never chosen
as a fallback.

Edits use registry revision checks. If another client changes the registry, the
editor retains its draft and asks you to reload or explicitly reconcile it.
Deleting a referenced tool or model is rejected.

## Plans and execution

The plan ledger stores drafts, published revisions, and explicit receipt
transitions. Published payloads are immutable. Compilation resolves exact model,
effort, executable, dependencies, and write scope against the pinned registry
revision. It does not start workers or create execution authority.

The paired native MCP server exposes binding-scoped catalog, draft, publication,
and approved-task operations. Each call verifies its original operator enrollment;
model-provided chat IDs cannot establish identity. Configure the same global
registry, plans, and authority stores used by the desktop, together with a private
sessions root. Store paths are trusted launch arguments, never MCP parameters.

A desktop operator can preview a published plan and approve a verified repository,
exact Git base commit, selected tasks, write paths, concurrency, and budgets.
Directory scopes end with `/`; bare scopes name exact files. Approval alone starts
neither a process nor a model. The separate **Start local session** action launches
or attaches to a dedicated `threadlane-host` process. It leaves the original
conversation observer attached to its control runtime. Closing the desktop does
not stop that separately owned host.

Once the host is authenticated and ready, the enrolled Codex follower can call
`start_approved_task` with only the approval and task IDs. The backend loads the
frozen instruction, model, effort, executable, scope, and idempotency key; the
caller cannot replace them. Independent root AGY/OpenCode tasks are supported.
Dependent tasks stay blocked until verified predecessor inputs are implemented;
Codex roles remain native/manual routes.

The legacy trusted startup-profile route remains documented in
[DISPATCH.md](DISPATCH.md). Native two-chat isolation and busy-turn delivery still
require live acceptance. A stored receipt alone does not prove that an external
orchestrator ran. Separate approval hosts expose read-only activity through MCP
and the Plans session panel. An explicit startup-only opt-in can also queue a
fixed approval wake-up to the original Codex chat; it is off by default. The new
`approved_pull_events` / `approved_ack_events` tools deliver and acknowledge that
host's ordered stream without changing the control binding's cursors. See
[approved-session notifications](APPROVED-NOTIFICATIONS.md) for configuration,
recovery and the live pairing/notification acceptance gates.

CLI workers support bounded cancellation and safe replacement after a proven stop.
In-flight provider steering is not implied. Uncertain outcomes block replacement
instead of starting duplicate work.

## Data locations

The native application selects its registry under its app configuration directory
and its plans under its app data directory. A trusted CLI launch may set
`--registry-dir`; JavaScript cannot choose arbitrary storage roots. Plans,
approvals, and host sessions live in separate subdirectories of application data.
The Nix desktop wrapper supplies its matching `threadlane-host` companion on its
own process PATH. Development launches may use the trusted `--host-executable`
argument; it is not exposed to JavaScript. Credentials remain in the OS keychain,
outside those JSON stores.

The runtime's `state.db`, supervisor descriptor, logs, and task worktrees remain
separate from configuration preferences and source control.

## Execution approval foundation

An approval freezes the exact published revision, registry version, model/effort
choices, repository, base commit, task instructions, write scopes, and budgets.
It is an operator action, not an interpretation of worker text. The immutable
approval and host boundaries are implemented and covered by local fixture tests.
A successful worker is only **Ready for review**, with `accepted: false`; it never
automatically merges, pushes, cleans worktrees, or marks a plan applied. Consult
the current [checkpoint](M4-CHECKPOINT.md) for live acceptance limits.

AGY and OpenCode are the current headless worker providers. Codex roles use native
conversation routes. Dependent tasks require verified predecessor inputs; worker
completion alone does not establish that those inputs have been integrated.

## Activity and settings

**Activity** observes an attached runtime. Expand **History** to filter recorded
events by run or event kind and inspect details. Reading history does not consume
the follower's event delivery or acknowledgement cursor.

**Settings** controls appearance and bounded execution preferences. Changing a
preference does not rewrite a running worker's immutable startup configuration.
Choose the system theme or an explicit theme, an accent, and an opaque or glass
surface. Native blur availability depends on the desktop platform; text remains
opaque when changing the surface opacity.

The browser-only `?preview=1` mode is a development fixture. Its synthetic tools,
profiles, demo runs, and history are not a connection to the real runtime. Ordinary
browser windows without the Tauri bridge report that connection as unavailable.
