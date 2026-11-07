# OpenCode v2 metadata validation

`threadlane_workers::opencode_metadata::validate_opencode_metadata` is a pure
validator. It accepts already-loaded inputs and produces compact evidence or a
finite error code. It performs no file, process, network, session-discovery or
export operation. The worker supervisor now uses it through a bounded collector
for new software-owned OpenCode v2 sessions.

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

### Stage-qualified failure diagnostics

`validate_opencode_metadata` preserves its `Result<OpenCodeMetadataEvidence, OpenCodeMetadataError>`
signature, while `validate_opencode_metadata_with_diagnostic` returns
`OpenCodeMetadataDiagnostic` (`error` plus optional `OpenCodeMetadataStage`).
Semantic error codes that already identify the failure (`output_overflow`,
`ownership_mismatch`, `stale_session`, `missing_model`, `model_mismatch`,
`missing_effort`, `effort_mismatch`, `missing_completion`, `provider_error`,
`nonzero_exit`, `timed_out`, `cancelled`, `cleanup_incomplete`, `spawn_failed`,
`invalid_context`, `unsupported_platform`) keep `stage: None` and their exact
code string.

When session creation or metadata validation fails with `invalid_metadata`, the
collector and validator attach one finite `OpenCodeMetadataStage` and encode
`WorkerExecutionReport::opencode_metadata_error` as `invalid_metadata:<stage>`:

- `caller_contract`: invalid software-supplied session ID, non-canonical working directory, or unparseable/unsafe requested `provider/model` or `effort` target.
- `create_response_json`: `api --standalone session.create` stdout is not a top-level JSON object.
- `create_response_envelope`: `api --standalone session.create` is missing `data`, a safe `data.id`, `data.location.directory`, or `data.time.created`.
- `info_response_json`: `api --standalone session.get` stdout is not a top-level JSON object.
- `info_response_envelope`: `api --standalone session.get` is missing the `data` object.
- `export_json`: `session export --standalone --sanitize` stdout is not a top-level JSON object.
- `export_envelope`: `session export --standalone --sanitize` is missing the `info` object or `messages` array.
- `identity_workdir`: missing or non-string `id` in `info.data` or `export.info`, or missing/non-object `location` or non-string `location.directory` in `info.data`.
- `timestamps`: missing `time`/`time.created` in `info.data` or `export.info`, non-integer `created`/`idle`/`completed` timestamps, or mismatched `created` between `info.data` and `export.info`.
- `message_envelope`: non-object message entry, legacy nested `messages[].info.role` shape, missing/non-string message `type`, missing/non-string or unsafe message `id`, or duplicate message `id`.
- `assistant_target`: non-string `model.variant` or unsafe `model.providerID`, `model.id`, or `model.variant` identifier.
- `terminal_completion`: malformed non-string assistant `finish` field (while missing/non-`succeeded` `outcome`, missing/non-`stop` `finish`, and `completed > idle` remain `missing_completion`, and zero assistant messages remains `missing_model`).

Diagnostics never persist or echo raw JSON, text, titles, content, reasoning,
URLs, paths, IDs, tokens, or arbitrary provider error strings. Legacy
undifferentiated `invalid_metadata` reports remain readable via
`OpenCodeMetadataDiagnostic::from_error_code` and
`WorkerExecutionReport::opencode_metadata_diagnostic` with `stage: None`
(unknown/unavailable detail, never an invented stage). Stage diagnostics improve
future failure provenance; they do not prove the exact predicate or timing cause
of a historical `invalid_metadata` pilot whose rejected payload was intentionally
not retained, and they do not turn a failed run into success or authorize replay.

This is CLI-reported metadata, not cloud attestation. Parser tests alone do not
establish live provider compatibility.

## Owned execution collector

A new OpenCode job with no requested session first creates a fresh session using
its validated executable, canonical task directory and pinned child environment.
The returned identifier, directory and creation time must validate before model
execution. The effective run receives that exact session ID; the caller's
requested session remains distinct from the allocated and observed identity.

After a clean model-process exit, the collector reads only that owned session's
info and mandatory sanitized export. It never discovers sessions, queries an ID
from model prose, exports a caller-selected old session, falls back to raw export,
or replays inference after failure. Explicit existing-session jobs retain their
stream-only evidence path.

Each metadata command has a ten-second maximum and shares the original job's
absolute deadline. Info/create output is capped at 64 KiB and export at 1 MiB.
Both pipes drain concurrently; stderr is discarded. Cancellation, timeout,
overflow, read failure or incomplete descendant cleanup prevents acceptance.
Metadata and export content stay in memory; only allowlisted evidence or finite
error codes enter the report.

Reports preserve the original NDJSON evidence, including `saw_result_event`.
Validated per-assistant metadata may supplement missing model, effort or terminal
completion. Stream mismatches, blocks, errors and malformed output remain
failures. Runtime observation uses effective evidence without inventing a stream
result. A passing execution and diff gate yield **Ready for review**; they never
automatically accept or merge a patch.

## Validation

Synthetic unit and subprocess tests cover valid records and parameterized
negative cases, including finite `invalid_metadata:<stage>` provenance, legacy
`invalid_metadata` compatibility (`stage: None`), sentinel non-leakage, ownership,
staleness, model/effort, completion, budget, duplicate-ID and output-allowlist
failures across the validator, collector, and supervisor. The validator is also
included in the workspace test gate. No real conversation is necessary to run
these tests.
