# Installed tool versions

Threadlane checks enabled local CLI installations in the background after the
first successful registry read on each desktop startup. Tools shows the reported
CLI version and resolved executable. **Recheck versions** refreshes the same
metadata without restarting the app.

These checks use only `--version` (`tmux` uses `-V`). Disabled tools and API
adapters start no process. Each command has a three-second limit, bounded output
and process-group cleanup; at most four tool inspections run concurrently.
Model catalogs and login status still require their separate explicit actions.

## System and custom installations

An empty executable field means **System**: resolve the installation afresh.
An explicit path means **Custom**: keep that exact override. Threadlane can show
a different system candidate alongside a custom installation; a different path
alone does not establish that its version is newer.

**Use system** saves the automatic selection through the revision-checked
registry. Model and effort selections remain separate. Existing approvals keep
their frozen execution settings.

On NixOS, stable profile entries take precedence over stale direct store paths.
If a standalone Codex CLI is unavailable, discovery inspects the current desktop
launcher's matching `resources/codex` without running the desktop launcher or
any sourced shell code. Supported literal wrappers, symlinks and package metadata
are bounded and validated. An unsupported installed bundle requires attention
instead of guessing another store path. Other operating systems retain their
own installation and runtime acceptance requirements.

The desktop package version and bundled Codex CLI version are separate values.
An already running Codex app can continue using its old backend until its next
launch, even when a newer installation is present.

## Updates and UI behaviour

Version inspection does not download packages or run an installer. NixOS users
continue updating through their system configuration. Background results are
tied to the registry revision and cannot overwrite newer selections. Tool form
drafts, keyboard focus and scroll position survive a background version result.

Validation includes isolated layout changes, custom-pin preservation, stale
responses, disabled/API adapters, fixed command arguments, concurrency, timeout
cleanup, and the real installed Linux/Nix package layout. Fixture coverage is
not a macOS or Windows runtime acceptance claim.
