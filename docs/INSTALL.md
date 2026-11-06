# Installation and packaging

This document describes the source-build and Nix packaging lane for Threadlane.
It is deliberately not a system installer and does not create an OS service,
system package, AppImage, deb, or signed release. Nix is used to build from this
checkout; the existing development shells remain the normal development path.

## Packaging status

| Area | State in this lane |
| --- | --- |
| Python desktop build wrapper | Ten offline fixture tests pass. |
| Nix CLI package | Source build passed using the root `Cargo.lock`. Includes `threadlane`, the paired `threadlane-native-mcp` bootstrap, and the trusted `threadlane-context`, `threadlane-workflow`, `threadlane-session`, approval and operator commands. |
| Nix desktop package | Linux source build passed with the standalone Tauri lockfile, a separately built frontend, and explicit Cargo build subdirectory. The resulting executable's `--help` passed. The latest native window creation was observed; visual human acceptance of the current window is still pending. |
| GitHub Actions workflows | `.github/workflows/ci.yml` runs automatic Linux source validation by calling the reusable packaging workflow. `.github/workflows/packaging.yml` also provides the manual full cross-platform matrix and artifacts. No hosted run of either workflow has been observed yet, so hosted validation remains pending. |
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
  usual C/C++ build tools. The packaging workflow installs the corresponding
  Ubuntu development packages for its Linux jobs.

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
The desktop package is not exposed on Darwin. The Nix package provides only the
desktop executable and its small launcher environment helper, with the compiled
frontend embedded in the executable. It does not install an OS service, desktop
autostart entry, driver setting, or system-wide configuration.

The desktop binary is wrapped with the Linux GTK/WebKit runtime environment:

- `LD_LIBRARY_PATH` includes the Nix GTK/WebKit and supporting libraries.
- `GIO_EXTRA_MODULES` includes `glib-networking`.
- `XDG_DATA_DIRS` includes the GSettings schemas and GTK data directories.

On a native Wayland session with a readable `/proc/driver/nvidia/version` and no
explicit `GDK_BACKEND=x11`, the wrapper also defaults
`__NV_DISABLE_EXPLICIT_SYNC=1` when that variable is unset. An operator value,
including `0` or an empty value, is preserved. The helper text is packaged as a
standalone store file, so the runtime closure does not include the source
checkout. The default applies only to the packaged `threadlane-desktop`
launcher; the development shells, source build wrapper, and CLI package are
unchanged.

These are process-local wrapper settings. The package does not force X11, set
`WEBKIT_DISABLE_DMABUF_RENDERER`, or set an NVIDIA or compositor workaround
globally.

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

The packaged desktop environment helper has separate offline fixtures:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p test_desktop_environment.py -v
```

They source the actual helper in a controlled POSIX shell with temporary NVIDIA
driver fixtures; they do not read the real GPU, start a desktop session, or
touch the network.

## CI workflows

Two workflows cover source validation. Neither publishes a release, signs an
artifact, uses repository secrets, creates a system installer, or modifies a
host. No hosted run has been observed from this checkout, so all hosted results
remain pending until a real run is recorded here.

### Automatic Linux source validation

`.github/workflows/ci.yml` has the display name **Linux CI**. It runs on pull
requests targeting `main`, pushes to `main`, and manual dispatch. Its single job
calls the local reusable workflow `./.github/workflows/packaging.yml` at the same
revision with:

```yaml
linux_only: true
skip_artifacts: true
```

The run validates `ubuntu-24.04` / `linux-x86_64` only. It keeps the frontend
`npm ci`, production build, and tests; both Rust workspaces' formatting, Clippy,
and serial tests; and the offline Python build-wrapper tests. It does not run the
desktop release wrapper or the CLI release build, and it does not stage or upload
artifacts. The caller uses `contents: read` permissions and does not inherit
repository secrets; the called workflow disables checkout credential
persistence. Its concurrency group is distinct from the packaging workflow's
group and cancels an older run for the same pull request or ref. There is no path
filter, so a required check is always scheduled for a matching pull request.

### Manual cross-platform packaging

`.github/workflows/packaging.yml` has the display name **Packaging validation**.
The same file also supports manual dispatch. Open the repository's **Actions**
tab, select **Packaging validation**, and run the workflow. Manual dispatch keeps
the full `fail-fast: false` matrix and uploads artifacts; the reusable inputs are
only supplied by `ci.yml`.

| Runner | Artifact label |
| --- | --- |
| `ubuntu-24.04` | `linux-x86_64` |
| `macos-14` | `macos-aarch64` |
| `windows-2022` | `windows-x86_64` |

Each job is configured to install the official Rust toolchain, Node.js 22, and
Python 3.12. It runs the frontend production build and `npm test`, which
recursively discovers every `apps/desktop/src/**/*.test.ts` file, including the
`src` root and nested directories, without a shell glob. The test command is
implemented by `apps/desktop/scripts/run-tests.mjs`; its small offline
discovery/exit-status regression test is part of the same run. The command keeps
`--experimental-strip-types`, uses Node's normal per-file isolation, fails if
discovery finds no files, and propagates a failing test exit status. On Node.js
24 only, `npm --prefix apps/desktop test -- --test-isolation=none` runs the
discovered files in one process for sandbox diagnostics; it is not the default.

The job then runs root workspace formatting, Clippy, and serial tests, followed
by the standalone `apps/desktop/src-tauri` workspace's formatting, Clippy, and
serial tests with `CARGO_TARGET_DIR=target/desktop`. That target directory is
separate from the root workspace's `target/` directory. It finishes with the
offline packaging tests, the desktop release wrapper, the CLI release build,
artifact staging, and a short-lived workflow artifact upload. The automatic Linux
caller reuses the same steps but skips only those final release and artifact
steps.

On a POSIX shell, the equivalent local validation commands are:

```sh
npm --prefix apps/desktop ci
npm --prefix apps/desktop run build
npm --prefix apps/desktop test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked -- --test-threads=1
cargo fmt --manifest-path apps/desktop/src-tauri/Cargo.toml -- --check
CARGO_TARGET_DIR=target/desktop cargo clippy --manifest-path apps/desktop/src-tauri/Cargo.toml --all-targets --locked -- -D warnings
CARGO_TARGET_DIR=target/desktop cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked -- --test-threads=1
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_build_desktop.py -v
```

The workflows do not publish a release, sign an artifact, use repository
secrets, or create a system installer. The action versions, hosted runner images,
and dependency installations are written as a reproducible starting point; their
actual behavior remains unverified until a real run is recorded.

## Linux runtime notes

The packaged desktop wrapper applies the verified NVIDIA workaround as a
process-local default. It does so only when the session is native Wayland
(`WAYLAND_DISPLAY` is nonempty or `XDG_SESSION_TYPE=wayland`), the NVIDIA driver
version node `/proc/driver/nvidia/version` is readable, and `GDK_BACKEND` is
not explicitly `x11`.

In that case it sets:

```sh
__NV_DISABLE_EXPLICIT_SYNC=1
```

only if `__NV_DISABLE_EXPLICIT_SYNC` is unset. Any operator-provided value is
preserved, including `0` and an empty value. Override the default explicitly
for one invocation when needed:

```sh
__NV_DISABLE_EXPLICIT_SYNC=0 nix run .#threadlane-desktop
```

Do not try `WEBKIT_DISABLE_DMABUF_RENDERER=1` first. On the affected NVIDIA host
it avoided the startup crash but left the window fully transparent, with only
hovered elements repainting. Reserve it for a host where the DMABUF renderer is
confirmed to be the specific failing layer; see WebKit bug 280210
(<https://bugs.webkit.org/show_bug.cgi?id=280210>).

The helper does not force X11, change compositing, or write a global environment,
driver, or system setting. Other GPU, compositor, and platform combinations
remain untested.

## Platform status

| Platform | Nix package | Source/CI build |
| --- | --- | --- |
| Linux x86_64 | CLI and desktop Nix builds pass; current-window visual acceptance is pending. | Standalone desktop build and Clippy pass. Automatic Linux validation and manual packaging have not run on hosted CI. |
| Linux aarch64 | Defined for CLI and desktop; not built. | No workflow matrix entry yet. |
| macOS | CLI expression only; desktop Nix package not provided. | Manual packaging-workflow entry has not run on hosted CI. |
| Windows | No Nix package. | Manual packaging-workflow entry has not run on hosted CI; Windows runtime ACL support remains a separate product concern. |

No successful CLI submission, generated workflow, or Nix expression is evidence
that a binary was built, installed, run, signed, or released. Hosted workflow
results remain pending until the repository records an actual run.
