# Packaged operator context command

Date: 2026-10-31; advice family added 2026-11-01. This document describes
`threadlane-context`, the packaged trusted local operator context command in
`threadlane-native`. It routes everyday workflow, approved-session, flow, and
Jev shadow-advice CLI invocations through an existing private version-1 context
JSON file without shells or ad-hoc wrapper scripts.

## Authority boundary

The private context file is a trusted local target selection. Validating the
context and confirming `(binding_id, thread_uuid)` against the running
authenticated supervisor proves only that the bound target exists in the live
local control plane. It does **not** authenticate per-call native chat identity
(`native_per_call_identity_verified: false`), establish automatic two-chat
routing, or grant human scope approval (`human_approval_verified: false`).

- Operational context commands are `check`, `workflow`, `session`, `flow`, and
  `advice`. Context-free `help workflow`, `help session`, `help flow`, and
  `help advice` expose sibling help only.
- The `flow` family is an additional forwarding surface for the
  `threadlane-flow` package sibling. It adds no scheduling, automatic routing,
  native identity, or human approval authority; the sibling owns its own
  behavior and remains subject to the same explicit operator boundaries.
- The `advice` family forwards to the `threadlane-advice` sibling. It adds no
  automatic classification or plan/profile/model change: the sibling records
  advisory-only shadow output for explicit commands. Only the sibling's
  `classify` subcommand may create the derived private advice cache; the context
  router never creates it. See [Jev shadow advice](JEV-SHADOW.md).
- Scope approval (`threadlane-approve`), host startup (`threadlane-host`), and
  review/integration (`threadlane-operator`) remain separate explicit operator
  commands and are not exposed as context pass-through families.
- The context router itself performs no setup/import wizard, store creation,
  permission repair, daemon startup, MCP registration, model change, or provider
  call. A forwarded sibling may perform its own explicit operation;
  for example, `threadlane-advice classify` may call Jev and create its derived
  cache.

## Commands

Outer `--help` / `-h` and `--version` / `-V` succeed without a context file or
running supervisor.

### Context-free family help

The four exact offline family help commands are:

```sh
threadlane-context help workflow
threadlane-context help session
threadlane-context help flow
threadlane-context help advice
```

All four forms succeed without a context file or a running supervisor. Each
command resolves only the canonical `threadlane-workflow`, `threadlane-session`,
`threadlane-flow`, or `threadlane-advice` executable in the current binary's
canonical parent directory and passes exactly one fixed `--help` argument. The
sibling's own help text is written through inherited stdio. `PATH` is never
consulted, and missing, symlink, non-regular, or non-executable package siblings
are refused. Any other family, extra argument, context-bearing form, protected
scope or context override, or general help pass-through is rejected before a
child is invoked.

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

threadlane-context --context /absolute/private/context.json \
  advice classify --request /absolute/private/request.json
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
  As with workflow, session, and advice, the private context and live binding
  are validated before child invocation; the router does not interpret or
  schedule any flow subcommand.
- `advice <args...>` resolves only the canonical `threadlane-advice` executable
  in the current binary's canonical parent directory, prepends exactly
  `--registry-dir` (from `stores.registry_dir`), `--cache-dir` (the
  `plans_dir/advice-cache` child), `--project-root`, and `--context-id` (the
  context `thread_uuid`) in that order, and forwards `<args...>` directly via
  `argv`. The private context and live binding are validated before child
  invocation, but the router does not interpret `preview`, `models`, or
  `classify`. Only the sibling's explicit `classify` may create the derived
  cache; `help advice` remains an offline sibling help command. The context
  schema and legacy serialization are unchanged.

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

The advice cache is derived from `plans_dir`; it is not another context store,
and context validation does not create it.

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
  `session`, `flow`, or `advice`).
- Forwarded overrides for `--state-dir`, `--registry-dir`, `--plans-dir`,
  `--authority-dir`, `--sessions-dir`, `--binding`, `--thread`, `--context`,
  `--project-root`, `--cache-dir`, and `--context-id` (including `=` forms and
  repeated flags) are rejected before child execution. For `advice`, all four
  prepended scope flags are derived from the context; the caller cannot replace
  the registry, cache, project root, or context ID.
- Sibling executables (`threadlane-workflow`, `threadlane-session`,
  `threadlane-flow`, and `threadlane-advice`) are resolved exclusively from the
  canonical parent directory of `current_exe()`. Missing (`not_configured`),
  symlink, non-regular, or non-executable (`security_violation`) children are
  rejected. The context's legacy package metadata and `PATH` never select a
  child executable.
- Child processes inherit normal `stdin`, `stdout`, and `stderr`, and their exit
  status is preserved.
- Spawn failure before a child process exists returns a static, sanitized error.
  Wait failure or signal termination after spawn returns `outcome_unknown` and
  never replays the command; reconcile workflow, session, flow, or advice state
  before retrying. For advice, that includes a pending attempt whose provider
  outcome is uncertain; it requires an explicit `advice classify ... --refresh`
  invocation rather than an automatic replay.

## Verification

Focused behavioral fixtures live in
[`crates/threadlane-native/tests/context_cli.rs`](../crates/threadlane-native/tests/context_cli.rs)
and exercise real compiled CLI subprocesses against local supervisor, workflow,
session, and authority stores plus test-owned argv-recording flow and advice
siblings without paid provider calls. The advice fixtures cover canonical scope
argument order, protected override forms, offline sibling help, legacy families,
and spawn/validation failures.
