# Installation and packaging

This document describes the source-build and Nix packaging lane for Threadlane.
It is deliberately not a system installer and does not create an OS service,
system package, AppImage, deb, or signed release. Nix is used to build from this
checkout; the existing development shells remain the normal development path.

## Packaging status

| Area | State in this lane |
| --- | --- |
| Python desktop build wrapper | Ten offline fixture tests pass. |
| Nix CLI package | Source build passed using the root `Cargo.lock`. Includes `threadlane` and the paired `threadlane-native-mcp` bootstrap. |
| Nix desktop package | Linux source build passed with the standalone Tauri lockfile, a separately built frontend, and explicit Cargo build subdirectory. The resulting executable's `--help` passed; GUI rendering through this Nix wrapper remains unverified. |
| GitHub Actions workflow | Manual `workflow_dispatch` source-build matrix added. It has not been dispatched from this checkout, so all three platform results remain unverified. |
| Desktop runtime | The standalone M4 Linux binary builds and passes all-target Clippy. The earlier native Wayland preview rendered successfully. Current Tools/Profiles and History browser checks use explicit DEV fixtures; they do not prove native chat pairing. |

The locked npm dependency hash is recorded in:

```text
nix/packages.nix: desktopNpmDepsHash
```

Only recompute it after `apps/desktop/package-lock.json` changes. Build
`nix build .#threadlane-desktop.frontend --no-link` to verify the frontend
independently. The Rust packages use their committed Cargo lockfiles.

Flake evaluation follows the Git-tracked source set. Stage newly added packaging
files and any new workspace crates before running the first Nix build; untracked
files are not part of a flake source tree.

## Requirements

- Nix with flakes enabled.
- Linux for the Nix desktop package. The CLI package is also defined for the
  supported Darwin systems.
- For source builds outside Nix: a current stable Rust toolchain with
  `cargo`, `rustfmt`, and `clippy`, Node.js 22 with npm, Python 3.10 or newer,
  and the native Tauri libraries for the host platform.
- Linux Tauri builds need GTK 3, WebKitGTK 4.1, libsoup 3, librsvg, and the
  usual C/C++ build tools. The manual CI workflow installs the corresponding
  Ubuntu development packages.

## Development shells (unchanged)

The default shell and the existing desktop shell keep their previous packages
and hooks:

```sh
nix develop            # core Rust/CLI work
nix develop .#desktop  # Tauri, Node.js, GTK/WebKit libraries
```

The desktop shell hook still supplies `GIO_EXTRA_MODULES` and `XDG_DATA_DIRS`
on Linux. It does not override `LD_LIBRARY_PATH` or force a compositor backend.

## Nix packages and entrypoints

Build or run the CLI:

```sh
nix build .#threadlane
nix run .#threadlane -- --help
```

Build or run the Linux desktop package:

```sh
nix build .#threadlane-desktop
nix run .#threadlane-desktop
```

`nix run .#threadlane-desktop` starts a GUI; a graphical session is required.
The desktop package is not exposed on Darwin. The Nix package installs only the
desktop executable, with the compiled frontend embedded in it. It does not
install an OS service, desktop autostart entry, driver setting, or system-wide
configuration.

The desktop binary is wrapped with the Linux GTK/WebKit runtime environment:

- `LD_LIBRARY_PATH` includes the Nix GTK/WebKit and supporting libraries.
- `GIO_EXTRA_MODULES` includes `glib-networking`.
- `XDG_DATA_DIRS` includes the GSettings schemas and GTK data directories.

These are process-local wrapper settings. The package does not force X11 and
does not set an NVIDIA or compositor workaround globally.

## Source desktop build wrapper

`scripts/build-desktop.py` is a shell-free developer and CI wrapper around the
existing standalone Tauri workspace. It resolves all paths from the checkout
root, runs npm with an argv list, and runs Cargo with an explicit manifest and
an absolute `CARGO_TARGET_DIR`:

```sh
# Show the exact commands and paths without creating files or running tools.
python3 scripts/build-desktop.py --dry-run

# Debug build, matching the existing desktop preview path.
python3 scripts/build-desktop.py

# Release build.
python3 scripts/build-desktop.py --release

# Set a per-step timeout (default 1800 seconds, maximum 7200).
python3 scripts/build-desktop.py --release --timeout-seconds 3600
```

The wrapper performs these steps in order:

1. `npm ci` in `apps/desktop`.
2. `npm run build` in `apps/desktop`.
3. `cargo build --manifest-path apps/desktop/src-tauri/Cargo.toml --locked`
   (plus `--release` when requested) with
   `CARGO_TARGET_DIR=<checkout>/target/desktop`.

It does not invoke a shell, a Tauri bundler, a signing tool, or an installer. It
removes inherited signing variables and sanitizes Git environment variables for
child processes. The expected executable is:

```text
target/desktop/debug/threadlane-desktop
target/desktop/release/threadlane-desktop
```

The wrapper verifies that the executable exists (and is executable on Unix)
before reporting success. Failures are JSON objects with a bounded, redacted
diagnostic and a nonzero exit code.

Run the offline wrapper regressions with:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_build_desktop.py -v
```

The tests use temporary checkout layouts and local Python fixture executables;
they do not call npm, Cargo, a provider, or the network.

## Manual cross-platform CI

The workflow `.github/workflows/packaging.yml` is manual-only. Open the
repository's **Actions** tab, select **Packaging validation (manual)**, and run
the workflow. It uses a `fail-fast: false` matrix for:

| Runner | Artifact label |
| --- | --- |
| `ubuntu-24.04` | `linux-x86_64` |
| `macos-14` | `macos-aarch64` |
| `windows-2022` | `windows-x86_64` |

Each job installs the official Rust toolchain, Node.js 22, and Python 3.12,
then runs frontend build/tests, Rust formatting/Clippy/tests, the offline
packaging tests, and the desktop wrapper. It uploads the CLI, desktop executable,
and frontend bundle as a short-lived workflow artifact.

The workflow does not publish a release, sign an artifact, use repository
secrets, or create a system installer. The action versions, hosted runner images,
and dependency installations are written as a reproducible starting point; their
actual behavior remains unverified until a real dispatch is recorded.

## Linux runtime notes

The milestone 3A preview showed a Wayland protocol error on the first NVIDIA
host. The verified workaround was process-local:

```sh
__NV_DISABLE_EXPLICIT_SYNC=1 nix run .#threadlane-desktop
```

If WebKit's DMABUF renderer is the specific failure on a host, this alternative
is also process-local:

```sh
WEBKIT_DISABLE_DMABUF_RENDERER=1 nix run .#threadlane-desktop
```

Do not make either setting global, and do not force X11 by default. Other GPU,
compositor, and platform combinations still need their own smoke tests.

## Platform status

| Platform | Nix package | Source/CI build |
| --- | --- | --- |
| Linux x86_64 | Locked frontend build passes; complete package validation is in progress. | Standalone desktop build and Clippy pass. Manual workflow not yet run. |
| Linux aarch64 | Defined for CLI and desktop; not built. | No workflow matrix entry yet. |
| macOS | CLI expression only; desktop Nix package not provided. | Manual workflow entry not yet run. |
| Windows | No Nix package. | Manual workflow entry not yet run; Windows runtime ACL support remains a separate product concern. |

No successful CLI submission, generated workflow, or Nix expression is evidence
that a binary was built, installed, run, signed, or released.
