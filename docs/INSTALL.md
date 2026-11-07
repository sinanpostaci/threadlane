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
| GitHub Actions workflows | All 902 default tests, separate doctest checks, Python helper tests and workflow syntax checks pass locally. Four bounded Nextest shards replace the monolithic root test step. Hosted qualification of this layout and manual packaging remain pending; see [CI workflows](#ci-workflows). |
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
host. Three automatic Linux runs have now stopped without a complete pass. The
[first run](https://github.com/sinanpostaci/threadlane/actions/runs/42235190924)
failed while compiling Rust test binaries because the runner ran out of disk
space; no test assertion was established. The
[second run](https://github.com/sinanpostaci/threadlane/actions/runs/42239119291)
at head `58231ed3493d00c1a8813f3a4122311c6ee666f3` passed the frontend
build/tests, both formatting checks, and both Clippy checks, then ran the root
tests and failed in `worker_and_git_foundation`: 23 passed / 1 failed in
`escaped_pipe_descendant_bounded_join_aborted_future_raii_kill_and_false_cancel_ignored`,
at the assertion `escaped pipe holder must mark cleanup_incomplete = true`. Its
final disk report showed 8.0 GB free, so this failure was not disk exhaustion.
The explicit-Bash fixture correction and independent escape proof passed the
local focused checks (escaped scenario 1/1, full foundation suite 24/24,
formatting, and worker Clippy with `-D warnings`).

The [third run](https://github.com/sinanpostaci/threadlane/actions/runs/42256529293)
passed the frontend build/tests, both formatting checks, the early 24-worker
cleanup gate and both Clippy steps before reaching the job timeout during root
Rust tests.

The [subsequent run](https://github.com/sinanpostaci/threadlane/actions/runs/42296276421)
lost runner communication on its first attempt. Its second attempt passed the
same early checks and the offline Python CI-wrapper tests, then GitHub
automatically cancelled it at the 90-minute job limit during root workspace
tests. Hosted logs for the timed-out root step are unavailable. No specific
hanging test or defect in `scripts/run-ci-check.py` has been established.

To make per-test duration, timeouts, and failures visible within standard public
Linux runner budgets, `.github/workflows/ci.yml` now separates automatic Linux
validation into the existing reusable quality/frontend/Tauri job (`45m` cap,
with `skip_root_tests: true`) plus four parallel `ubuntu-24.04` root-test shard
jobs (`35m` cap each) using pinned `cargo-nextest` `0.9.151` and explicit
doctest execution on shard 1. The long CLI/driver integration scenario has
sequential 90s and 300s internal waits. Read-only owned-fixture observations
confirmed actual stage progression, and the exact test passed under its
eight-minute exception. This does not establish the root cause of earlier
hosted timeouts. Hosted sharded runs, manual packaging, macOS/Windows and live
runtime acceptance remain unqualified.

Local validation covered all **902 default tests** without repeating successful
cases: 561 passed before a bounded single-group comparison hit its 20-minute
global ceiling; four complementary partitions covered the remaining cases.
The one multi-phase driver case reached the initial five-minute limit and then
passed with the exact eight-minute override (about 306 seconds including command
setup). The successful test sets are disjoint and cover the complete default
inventory. The separate doctest command, 12 CI-wrapper tests, 11 packaging-wrapper
tests and workflow syntax checks also passed. This is local evidence, not a
successful hosted run of the new layout.

### Automatic Linux source validation

`.github/workflows/ci.yml` has the display name **Linux CI**. It runs on pull
requests targeting `main`, pushes to `main`, and manual dispatch. It schedules
two parallel job families on standard public `ubuntu-24.04` runners:

1. **`linux-source-validation`** (`45`-minute job cap): calls the local
   reusable workflow `./.github/workflows/packaging.yml` at the same revision
   with:

   ```yaml
   linux_only: true
   skip_artifacts: true
   skip_root_tests: true
   ```

   This job validates `ubuntu-24.04` / `linux-x86_64` only. It keeps the
   frontend `npm ci`, production build, and tests; both Rust workspaces'
   formatting and Clippy checks; the early `worker_and_git_foundation` cleanup
   regression gate; the standalone Tauri serial tests; and the offline Python
   build-wrapper and CI-wrapper tests (`test_build_desktop.py` and
   `test_run_ci_check.py`). It sets `skip_root_tests: true`
   only because the four sibling `linux-root-tests` shard jobs cover the root
   workspace test suite. It does not run the desktop release wrapper or the CLI
   release build, and it does not stage or upload release binary artifacts
   (`skip_artifacts: true`).

2. **`linux-root-tests`** (`35`-minute job cap per shard, `fail-fast: false`,
   `shard: [1, 2, 3, 4]`): runs four independent standard `ubuntu-24.04` jobs
   for the root Rust workspace. Because the desktop Tauri workspace is
   standalone, the root shards install only the minimal stable Rust toolchain
   and Python 3.12 (no Node.js, npm, or GTK/WebKit packages). Each shard sets
   `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`,
   `CARGO_INCREMENTAL=0`, and `CARGO_BUILD_JOBS=2`.

Each root shard downloads the official static musl archive for
`cargo-nextest` `0.9.151`
(`https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-0.9.151/cargo-nextest-0.9.151-x86_64-unknown-linux-musl.tar.gz`)
into `$RUNNER_TEMP`, verifies its pinned SHA-256 digest
(`6b284d6fde9670dfb6ee10ea9d9abbc85f95643d3f592472d420d82e257b3a6c`) with
`sha256sum --check --strict -` before extracting only the `cargo-nextest`
binary member, removes the archive, and appends only that binary directory to
`GITHUB_PATH`. No floating third-party install action or source compilation of
Nextest is used.

Each shard executes its disjoint partition of the root workspace tests under
the outer `scripts/run-ci-check.py` wrapper (covering both compilation and test
execution with a 30-minute hard ceiling):

```sh
python3 scripts/run-ci-check.py --timeout-seconds 1800 -- cargo nextest run --workspace --locked --profile ci --partition hash:<shard>/4
```

Because `cargo nextest` does not execute Rust documentation tests, shard 1
additionally runs all workspace doctests under the same wrapper:

```sh
python3 scripts/run-ci-check.py --timeout-seconds 600 -- cargo test --workspace --doc --locked
```

`.config/nextest.toml` defines `[profile.ci]`, `[profile.ci.junit]`, and one
scoped `[[profile.ci.overrides]]` entry for the sharded runs:

- `test-threads = 2`, `retries = 0`, and `fail-fast = false`.
- `status-level = "pass"`, `final-status-level = "slow"`,
  `failure-output = "immediate-final"`, and `success-output = "never"`, so
  console logs show per-test `PASS`, `FAIL`, and `TIMEOUT` outcomes.
- `slow-timeout = { period = "30s", terminate-after = 10 }` (terminating and
  failing any test that exceeds 5 minutes by default, with no timeout-pass
  override), `global-timeout = "20m"`, and
  `leak-timeout = { period = "500ms", result = "fail" }`.
- One `[[profile.ci.overrides]]` rule matching
  `binary_id(=threadlane-native::coworker_pipeline) & test(=coworker_full_driver::cli_code_driver_full_automatic_pipeline_runs_parallel_roots_reviewer_correction_dependent_and_single_closure_to_completed)`
  with `slow-timeout = { period = "30s", terminate-after = 16 }` (a finite
  8-minute per-test ceiling). Its contract in
  `tests/support/coworker_full_driver.rs` sequentially performs a 90-second wait
  and a separate 300-second completion wait plus setup, duplicate ensure/status,
  and teardown checks, exceeding the general 5-minute budget without weakening
  assertions or raising global/default limits.
- `path = "junit.xml"` under `[profile.ci.junit]` writes
  `target/nextest/ci/junit.xml` with `store-success-output = false` and
  `store-failure-output = true`. Each shard always uploads only that structured
  XML report (`nextest-junit-shard-<shard>`, 7-day retention,
  `if-no-files-found: warn` in case compilation fails before Nextest writes the
  report). No source tree, runtime store, or raw passing logs are uploaded, and
  automatic release binary staging remains disabled.

The caller workflow uses `contents: read` permissions and does not inherit
repository secrets; checkout credential persistence is disabled in all jobs. Its
concurrency group is distinct from the packaging workflow's group and cancels an
older run for the same pull request or ref. There is no path filter, so required
checks are always scheduled for matching pull requests.

After both formatting checks, the `linux-source-validation` job still runs
`cargo test --locked -p threadlane-workers --test worker_and_git_foundation -- --test-threads=1`
as an early signal before root Clippy, followed by the offline CI-wrapper checks:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_run_ci_check.py -v
```

They use local Python fixtures and the standard library only; they do not
call Cargo, a provider, the network, or a system service. The quality job wraps
the unchanged standalone Tauri test argv with a 15-minute maximum and keeps
`CARGO_TARGET_DIR=target/desktop`:

```sh
CARGO_TARGET_DIR=target/desktop python3 scripts/run-ci-check.py --timeout-seconds 900 -- cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked -- --test-threads=1
```

If `.github/workflows/packaging.yml` is called in source-only mode without
`skip_root_tests: true`, its root Rust test step remains wrapped with a
60-minute hard maximum:

```sh
python3 scripts/run-ci-check.py --timeout-seconds 3600 -- cargo test --workspace --locked -- --test-threads=1
```

`scripts/run-ci-check.py` is POSIX-only and executes argv directly without a
shell. It streams stdout and stderr via nonblocking `os.read`, emits a fixed
heartbeat with elapsed time and the last libtest/build-phase hint, and enforces
only the explicit hard deadline; quiet output does not cancel a command. On
timeout it sends SIGTERM and then a bounded SIGKILL only while the original
child is still live, returns 124, and never reports success. It drains inherited
output pipes for a bounded interval after child exit and reports incomplete
cleanup nonzero without claiming escaped descendants were killed. This is CI
supervision only, not Threadlane model or runtime timeout policy. Manual and
non-Linux fallback steps keep the original exact cargo commands without the
wrapper and use the `(manual packaging)` label suffix.

Both the `linux-source-validation` quality job and the four `linux-root-tests`
shards set `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, and
`CARGO_INCREMENTAL=0` before Rust compilation. This removes DWARF and
incremental files without disabling debug assertions or changing optimization
levels. They also record `df -h .` before validation and in a final `always()`
step, so hosted runs capture disk capacity on success or failure. Manual
full-artifact runs keep Cargo's normal build defaults.

### Manual cross-platform packaging

`.github/workflows/packaging.yml` has the display name **Packaging validation**.
The same file also supports manual dispatch. Open the repository's **Actions**
tab, select **Packaging validation**, and run the workflow. Manual dispatch
keeps the 90-minute job timeout, the full `fail-fast: false` matrix, the serial
root and Tauri tests (`skip_root_tests` defaults to `false`), and release
artifact uploads (`skip_artifacts` defaults to `false`); the reusable inputs are
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

The job then runs root workspace formatting, Clippy, and (unless
`skip_root_tests: true` is passed by `ci.yml`) serial root tests, followed by
the standalone `apps/desktop/src-tauri` workspace's formatting, Clippy, and
serial tests with `CARGO_TARGET_DIR=target/desktop`. That target directory is
separate from the root workspace's `target/` directory. It finishes with the
offline packaging tests, the desktop release wrapper, the CLI release build,
artifact staging, and a short-lived workflow artifact upload. Both the
source-only and manual `(manual packaging)` root-test steps honor
`!inputs.skip_root_tests`, while manual full-artifact and non-Linux jobs keep
their original un-sharded `cargo test --workspace --locked -- --test-threads=1`
and Tauri test commands by default.

On a POSIX shell, the equivalent local validation commands are:

```sh
npm --prefix apps/desktop ci
npm --prefix apps/desktop run build
npm --prefix apps/desktop test
cargo fmt --all -- --check
cargo fmt --manifest-path apps/desktop/src-tauri/Cargo.toml -- --check
cargo test --locked -p threadlane-workers --test worker_and_git_foundation -- --test-threads=1
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo nextest run --workspace --locked --profile ci --partition hash:1/4
cargo nextest run --workspace --locked --profile ci --partition hash:2/4
cargo nextest run --workspace --locked --profile ci --partition hash:3/4
cargo nextest run --workspace --locked --profile ci --partition hash:4/4
cargo test --workspace --doc --locked
CARGO_TARGET_DIR=target/desktop cargo clippy --manifest-path apps/desktop/src-tauri/Cargo.toml --all-targets --locked -- -D warnings
CARGO_TARGET_DIR=target/desktop cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --locked -- --test-threads=1
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_build_desktop.py -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_run_ci_check.py -v
```

The manual packaging workflow still uses
`cargo test --workspace --locked -- --test-threads=1`, while automatic Linux CI
uses the four `cargo nextest` shards plus `cargo test --workspace --doc --locked`
under `scripts/run-ci-check.py` as shown in the previous section.

The workflows do not publish a release, sign an artifact, use repository
secrets, or create a system installer. Earlier hosted Linux runs stopped on disk
exhaustion, one worker cleanup assertion, and GitHub's 90-minute job timeout
during monolithic serial root tests with unavailable logs. All 902 default tests
passed locally across disjoint successful runs, including the extended-budget
multi-stage driver recheck. Hosted sharded runs, macOS/Windows qualification and
live runtime acceptance remain separate gates.

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
| Linux x86_64 | CLI and desktop Nix builds pass; current-window visual acceptance is pending. | Standalone desktop build and Clippy pass. Local root test-set, separate doctest, helper and workflow syntax checks pass. Hosted sharded qualification and manual packaging remain pending. |
| Linux aarch64 | Defined for CLI and desktop; not built. | No workflow matrix entry yet. |
| macOS | CLI expression only; desktop Nix package not provided. | Manual packaging-workflow entry has not run on hosted CI. |
| Windows | No Nix package. | Manual packaging-workflow entry has not run on hosted CI; Windows runtime ACL support remains a separate product concern. |

No successful CLI submission, generated workflow, or Nix expression is evidence
that a binary was built, installed, run, signed, or released. Earlier automatic
Linux runs stopped without a complete pass (disk exhaustion, one worker cleanup
assertion, and auto-cancellation at the 90-minute job limit during serial root
tests with unavailable logs). Local validation of the sharded Nextest + doctest
layout does not establish hosted workflow success, macOS/Windows support or live
runtime acceptance; those gates remain unverified.
