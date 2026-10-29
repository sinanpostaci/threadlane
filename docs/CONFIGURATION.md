# Tools, models, and workflow profiles

The desktop configuration registry is independent of a running supervisor. Saving a
tool or profile does not restart workers, change their model, or grant access to a
repository. A new native installation starts with an empty registry.

## Configure a CLI tool

1. Open **Tools**, then **Add tool**.
2. Choose an adapter and a unique ID. An executable path is optional when the
   command is available on `PATH`; explicit absolute paths may use normal symlinks.
3. Save the tool, open **Details**, then choose **Probe saved tool**.
4. Import selected catalog entries, or add a model manually. Keep the registry ID
   distinct from the actual provider argument.

A probe checks installed metadata. It does not prove subscription entitlement or
perform inference. Codex catalogs include effort metadata. OpenCode and Antigravity
catalogs may not; configure supported efforts explicitly instead of inferring them
from a model's display name.

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

Current execution still requires trusted supervisor startup profiles described in
[DISPATCH.md](DISPATCH.md). Connecting saved workflow preferences to an operator-
approved project session and native conversation is a separate integration gate.
Native multi-chat pairing and busy-turn delivery remain unverified until the live
host acceptance test passes. A stored receipt alone is not proof that an external
orchestrator ran.

CLI workers support bounded cancellation and safe replacement after a proven stop.
In-flight provider steering is not implied. Uncertain outcomes block replacement
instead of starting duplicate work.

## Data locations

The native application selects its registry under its app configuration directory
and its plans under its app data directory. A trusted CLI launch may set
`--registry-dir`; JavaScript cannot choose arbitrary storage roots. Credentials
remain in the OS keychain, outside those JSON stores.

The runtime's `state.db`, supervisor descriptor, logs, and task worktrees remain
separate from configuration preferences and source control.

## Execution approval foundation

An approval freezes the exact published revision, registry version, model/effort
choices, repository, base commit, task instructions, write scopes, and budgets.
It is an operator action, not an interpretation of worker text. The immutable
approval foundation is implemented; desktop approval, host startup, and native
workflow wiring are being integrated separately. Consult the current
[checkpoint](M4-CHECKPOINT.md) before treating those paths as accepted end to end.

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
