# Packaged operator context command

Date: 2026-10-31. This document describes `threadlane-context`, the packaged
trusted local operator context command in `threadlane-native`. It routes
everyday workflow, approved-session, and flow CLI invocations through an
existing private version-1 context JSON file without shells or ad-hoc wrapper
scripts.

## Authority boundary

The private context file is a trusted local target selection. Validating the
context and confirming `(binding_id, thread_uuid)` against the running
authenticated supervisor proves only that the bound target exists in the live
local control plane. It does **not** authenticate per-call native chat identity
(`native_per_call_identity_verified: false`), establish automatic two-chat
routing, or grant human scope approval (`human_approval_verified: false`).

- Operational context commands are `check`, `workflow`, `session`, and `flow`.
  Context-free `help workflow`, `help session`, and `help flow` expose sibling
  help only.
- The `flow` family is an additional forwarding surface for the
  `threadlane-flow` package sibling. It adds no scheduling, automatic routing,
  native identity, or human approval authority; the sibling owns its own
  behavior and remains subject to the same explicit operator boundaries.
- Scope approval (`threadlane-approve`), host startup (`threadlane-host`), and
  review/integration (`threadlane-operator`) remain separate explicit operator
  commands and are not exposed as context pass-through families.
- No context setup/import wizard, store creation, permission repair, daemon
  startup, MCP registration, model change, or provider call is performed.

## Commands

Outer `--help` / `-h` and `--version` / `-V` succeed without a context file or
running supervisor.

### Context-free family help

The three exact offline family help commands are:

```sh
threadlane-context help workflow
threadlane-context help session
threadlane-context help flow
```

All three forms succeed without a context file or a running supervisor. Each
command resolves only the canonical `threadlane-workflow`, `threadlane-session`,
or `threadlane-flow` executable in the current binary's canonical parent
directory and passes exactly one fixed `--help` argument. The sibling's own help
text is written through inherited stdio. `PATH` is never consulted, and missing,
symlink, non-regular, or non-executable package siblings are refused. Any other
family, extra argument, context-bearing form, protected scope or context
override, or general help pass-through is rejected before a child is invoked.

These offline forms are not operational context validation: they do not read or
validate a context file, stores, package pin, or live binding, and they do not
contact the supervisor. They are also not approval, do not start a supervisor
or host, and make no native identity or authorization claim; scope approval
remains a separate `threadlane-approve` operation.

### Operational invocations

All operational invocations require `--context` before the command family:

```sh
threadlane-context --context /absolute/private/context.json check

threadlane-context --context /absolute/private/context.json \
  workflow catalog --limit 10

threadlane-context --context /absolute/private/context.json \
  session session --approval-id apr_0123456789abcdef0123456789abcdef
```

- `check` validates the private context file, `project_root`, all five private
  store directories, optional `cli_package` pin, and the live `(binding_id,
  thread_uuid)` supervisor binding, then writes the bounded operator envelope to
  stdout:
  - `untrusted_data: true`
  - `route: "trusted_local_operator"`
  - `native_per_call_identity_verified: false`
  - `binding_id`
  - `data` (`schema_version`, `project_root`, `binding_id`, `thread_uuid`,
    `route`, `native_per_call_identity_verified: false`,
    `live_binding_verified: true`, `human_approval_verified: false`,
    `cli_package_pinned`, and `stores`)
- `workflow <args...>` resolves only the canonical `threadlane-workflow`
  executable in the current binary's canonical parent directory, prepends the
  context-bound `--state-dir`, `--registry-dir`, `--plans-dir`, `--binding`, and
  `--thread` flags, and forwards `<args...>` directly via `argv`.
- `session <args...>` resolves only the canonical `threadlane-session`
  executable in the current binary's canonical parent directory, prepends the
  context-bound `--state-dir`, `--registry-dir`, `--plans-dir`,
  `--authority-dir`, `--sessions-dir`, `--binding`, and `--thread` flags, and
  forwards `<args...>` directly via `argv`.
- `flow <args...>` resolves only the canonical `threadlane-flow` executable in
  the current binary's canonical parent directory, prepends exactly the same
  five context-bound stores as `session` (`--state-dir`, `--registry-dir`,
  `--plans-dir`, `--authority-dir`, and `--sessions-dir`) plus the `--binding`
  and `--thread` flags, and forwards `<args...>` directly via `argv`.
  As with workflow and session, the private context and live binding are
  validated before child invocation; the router does not interpret or schedule
  any flow subcommand.

From a Nix checkout, inspect the packaged command without installing it globally:

```sh
nix shell .#threadlane --command threadlane-context --help
```

## Context schema version 1

The context file is read with `operator_cli::read_request` (`O_NOFOLLOW |
O_NONBLOCK`, regular file, `0600` owner-only permissions, `<= 1 MiB`, no
symlink components, and unchanged inode/size across the read).

Required operational fields:

- `schema_version`: `1`
- `project_root`: existing canonical absolute non-symlink directory (normal
  shared directory permissions such as `0755` are permitted; never `chmod`ed)
- `binding_id`: validated binding identifier
- `thread_uuid`: canonical lowercase hyphenated non-nil UUID
- `route`: `"trusted_local_operator"`
- `native_per_call_identity_verified`: `false`
- `stores`: object with five existing canonical absolute non-symlink `0700`
  pairwise distinct and non-overlapping directories validated via
  `operator_cli::validate_dirs`:
  - `state_dir`
  - `registry_dir`
  - `plans_dir`
  - `authority_dir`
  - `sessions_dir`

Legacy installation metadata (including `cli_package`, `desktop_package`, and
XDG fields) and unknown non-operational metadata are accepted without granting
authority to select executables or override bound stores. Duplicate or malformed
operational fields and top-level store/target override fields fail closed. When
`cli_package` is present, it must be an existing canonical non-symlink directory
whose `bin` directory matches the current executable's canonical sibling
directory; it can never select an arbitrary program.
A retained `cli_package` pin must be refreshed deliberately when the CLI package
changes; a stale pin is rejected rather than selecting a different installation.

## Locked flags and child execution

All wrapper arguments must be valid UTF-8. Non-UTF-8 Unix file names in argv
are rejected with a static `invalid_input` error.

- `--context` is valid only before the family name (`check`, `workflow`,
  `session`, or `flow`).
- Forwarded overrides for `--state-dir`, `--registry-dir`, `--plans-dir`,
  `--authority-dir`, `--sessions-dir`, `--binding`, `--thread`, `--context`, and
  `--project-root` (including `=` forms and repeated flags) are rejected before
  child execution.
- Sibling executables (`threadlane-workflow`, `threadlane-session`, and
  `threadlane-flow`) are resolved exclusively from the canonical parent
  directory of `current_exe()`. Missing (`not_configured`), symlink,
  non-regular, or non-executable (`security_violation`) children are rejected.
  The context's legacy package metadata and `PATH` never select a child
  executable.
- Child processes inherit normal `stdin`, `stdout`, and `stderr`, and their exit
  status is preserved.
- Spawn failure before a child process exists returns a static, sanitized error.
  Wait failure or signal termination after spawn returns `outcome_unknown` and
  never replays the command; reconcile workflow, session, or flow state before
  retrying.

## Verification

Focused behavioral fixtures live in
[`crates/threadlane-native/tests/context_cli.rs`](../crates/threadlane-native/tests/context_cli.rs)
and exercise real compiled CLI subprocesses against local supervisor, workflow,
session, and authority stores plus a test-owned argv-recording flow sibling
without paid provider calls.
