# OpenCode v2 metadata validation

`threadlane_workers::opencode_metadata::validate_opencode_metadata` is a pure
validator. It accepts already-loaded inputs and produces compact evidence or a
finite error code. It performs no file, process, network, session-discovery or
export operation. The runtime worker collector does not use it yet.

## Trusted inputs

The caller supplies a software-owned session ID, an already canonical working
directory, the trusted start timestamp and the immutable resolved model target.
These values must come from the owned execution, not provider prose or an
unrelated user-selected conversation.

The public OpenCode v2 session-info response has a `data` object. Its session ID,
creation time and real directory must match the owned execution. The sanitized
transfer contains `info` and a `messages` array; sanitization may mask its
directory, so that copy cannot establish working-directory ownership.

Assistant records are flat: `type: "assistant"`, `model.providerID`, `model.id`,
`model.variant`, `finish` and `time.created`/`time.completed`. The older nested
`messages[].info.role` shape is rejected. Mutable session-level model selection
is not per-assistant execution evidence.

## Checks and output

The validator bounds session info at 64 KiB, the sanitized transfer at 1 MiB and
the message list at 512 entries. It rejects mismatched or stale identity,
conflicting supplied message-session IDs, duplicate message IDs, malformed
required fields, model/effort mismatch, missing completion and provider errors.
Both session outcomes must be `succeeded`; the final assistant must finish with
`stop`, and the real idle timestamp must cover every assistant completion.

The result contains only the owned session ID, observed model/effort, assistant
count and terminal assistant ID, plus the fixed labels
`opencode_sanitized_session_export` and `cli_reported_per_assistant`. It does not
retain prompts, content, reasoning, titles, tool bodies, error prose or paths.
Errors contain finite static codes, rather than copied provider data.

This is CLI-reported metadata, not cloud attestation. A future collector must
establish ownership, bounded acquisition, cancellation and privacy before passing
inputs here. The current worker's missing-evidence gate remains unchanged;
parser tests alone do not establish live OpenCode worker compatibility.

## Validation

Thirteen synthetic test cases cover valid records and parameterized negative
cases, including schema, ownership, staleness, model/effort, completion, budget,
duplicate-ID and output-allowlist failures. The validator is also included in the
workspace test gate. No real conversation is necessary to run these tests.
