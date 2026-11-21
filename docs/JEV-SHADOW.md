# Shadow difficulty advice (Jev and OpenAI Decisions)

Date: 2026-11-01; OpenAI Decisions explicit and `--profile-id` profile-bound
preview/classify support added 2026-11-21. `threadlane-advice` exposes the
existing `DecisionClient` to an explicit native planning step. It is packaged
from `crates/threadlane-decisions` and runs only when an operator invokes it.
Both explicit unscoped and `--profile-id` profile-bound `preview` and `classify`
requests support enabled Jev (`jev_api`) and OpenAI Decisions
(`openai_decisions_api`) registry tools and models. Only the `models` catalog
command remains scoped to Jev (`jev_api`) only (and does not accept
`--profile-id`).

## Authority boundary

Advice is shadow-only. A result records a suggestion and, when supplied and a
difficulty is returned, a comparison with a baseline difficulty. It never
changes a plan, profile, difficulty, model, effort, permission, approval, host,
worker, credential, or Git state.

- Only `preview`, `models`, and `classify` perform advice work. Outer `--help`
  and `--version` are entirely offline.
- There is no conversation listener, daemon, service, automatic provider
  fallback, app restart, or global MCP/NixOS configuration change.
- `classify` is one explicit single-attempt advisory request or one completed
  cache hit. It does not automatically retry, fall back across providers, select
  another provider or model, or infer a model from help or catalog listings.
- A confident answer still selects and executes nothing. Labels and context IDs
  are explicit local correlation, not authenticated native per-call identity and
  not proof of human approval.
- Confidence is not a correctness guarantee, and agreement with a baseline is
  not approval.

## Credentials and registry setup

Credentials use the existing desktop Tools workflow; this package adds no
credential entry command.

1. Add and save a Jev API (`jev_api`) or OpenAI Decisions API
   (`openai_decisions_api`) tool in **Tools**, then enable it. The tool ID is
   the credential and registry key used later by `threadlane-advice`.
2. Open the tool details and choose **Set credential**. The native application
   stores the secret in the operating system keychain under that tool ID. There
   is no plaintext fallback, and the input is cleared after the attempt.
3. Save explicit models for the tool in the registry (for Jev, load the API
   model catalog and select exact server-returned names; for OpenAI Decisions,
   register an explicit supported Decisions model owned by the tool, such as
   `gpt-6-luna` or a dated `gpt-6-luna-*` identifier).
4. Keep the registry private. `preview` and `classify` require the request to
   name the current registry revision and an enabled `jev_api` or
   `openai_decisions_api` tool with a supported model owned by that tool.

The advice CLI never accepts a secret in argv, environment, request JSON, output,
or cache. It reads only the keychain entry for the selected tool. A locked or
unavailable keychain fails with a sanitized error; no plaintext fallback is
attempted. Headless or Nix sessions without an unlocked keychain fail closed.

Jev credentials and OpenAI Decisions credentials are separate and keyed by
`tool_id`. A Codex ChatGPT sign-in is not an OpenAI Decisions platform
credential, and a Jev key is not an OpenAI Decisions credential.

## Provider resolution and Jev catalog selection

Authoritative upstream Jev references:

- <https://docs.typesafe.ai/api/system-one>
- <https://docs.typesafe.ai/api/list-models>
- <https://docs.typesafe.ai/primitives/choice>
- <https://docs.typesafe.ai/primitives/noul>
- <https://docs.typesafe.ai/confidence>

`models --tool-id ID` remains scoped to enabled `jev_api` tools and performs
`GET https://api.typesafe.ai/v1/models` with bearer authentication for the
account. Supplying an `openai_decisions_api` tool or any non-`jev_api` tool to
`models` is refused with `unsupported_adapter` before credential or network
access. The response defines the account catalog. The result contains only
models the server returned: it does not append a pinned default, guess a current
version, or invent `jev-latest`. The documented `jev-latest` alias is selectable
only when the server returns it. It is not an automatic fallback and does not
authorize changing an existing explicit model selection.

In the registry, an API decision model uses the tool's ID as `tool_id`,
`tool_id:model_arg` as its model `id`, and the exact provider model identifier
as `model_arg`. For both explicit unscoped and `--profile-id` profile-bound
`preview` and `classify`, `threadlane-advice` resolves an enabled tool whose
adapter is `jev_api` or `openai_decisions_api` and a model owned by that tool
from a read-only `registry.json` snapshot. It derives the provider (`"jev"` or
`"openai"`) and `model_arg` from that snapshot; the request cannot override
either. When `--profile-id` is supplied, the saved workflow profile's shadow
advisor model must also permit no effort (`allow_no_effort: true`), its owning
tool must be an enabled `jev_api` or `openai_decisions_api` tool, and the
request's `tool_id` and `model_id` must match that saved selection exactly. A
mismatched expected registry revision, a disabled tool, an unsupported adapter,
a model that does not belong to the tool, a profile/request mismatch, or a
`model_arg` unsupported by the resolved provider (for example, a Jev model
under an OpenAI Decisions tool, an unsupported non-Decisions OpenAI model such
as `gpt-5-nano`, or an OpenAI model under a Jev tool) is refused before cache
creation, credential lookup, or API access. Existing explicitly configured model
choices are not rewritten. Neither Jev nor OpenAI Decisions advice uses an
effort parameter.

`models` requests metadata only. It never imports or mutates the registry, and it
does not perform classification.

`classify` sends one bounded single-attempt request through the existing
`DecisionClient`:

- For `jev_api`: `POST https://api.typesafe.ai/v1/systemone` with bearer
  authentication, the selected model, the task summary as state, and a Choice
  question over exactly the four difficulty levels.
- For `openai_decisions_api`: `POST https://api.openai.com/v1/decisions` with
  bearer authentication, the selected supported Decisions model (`gpt-6-luna` or
  `gpt-6-luna-*`), the task summary as input, and a Choice question over exactly
  the four difficulty levels.

It attaches no effort, alternate model, or provider fallback. The summary can be
sent only by an explicit `classify`; `preview`, `models`, `help`, and `version`
do not send it.

## Commands

Direct forms of the binary in `crates/threadlane-decisions`:

```text
threadlane-advice --help
threadlane-advice --version
threadlane-advice --registry-dir ABS --cache-dir ABS --project-root ABS --context-id ID preview --request ABS
threadlane-advice --registry-dir ABS --cache-dir ABS --project-root ABS --context-id ID classify --request ABS [--refresh]
threadlane-advice --registry-dir ABS --cache-dir ABS --project-root ABS --context-id ID models --tool-id ID
```

An optional global `--cache-ttl-seconds N` before the subcommand configures the
completed-result cache lifetime. The default is 3600 seconds; the accepted
maximum is 86400 seconds. Values outside the accepted bound are rejected.

An optional `--profile-id ID` binds `preview` or `classify` to that profile's
configured shadow decision advisor (an enabled `jev_api` or enabled
`openai_decisions_api` advisor model that permits no effort). It routes by the
saved advisor model's enabled owning adapter and requires the request's explicit
`tool_id` and `model_id` to match that saved selection exactly. An off, missing,
mismatched (including cross-provider Jev versus OpenAI Decisions), disabled,
effort-requiring, unsupported-adapter, or unsupported-model profile selection is
refused before cache creation, vault, or provider access, and no provider is
ever automatically selected or used as a fallback. `models` does not accept a
profile selection and remains scoped to enabled `jev_api` tools only. See
[profile decision advisors](PROFILE-ADVISOR.md). Without `--profile-id`,
existing Jev version-1 cache keys and pending-attempt protections remain
unchanged, and `default_profile_id` is never auto-selected.

- `--registry-dir` selects the read-only registry snapshot.
- `--cache-dir` selects the private completed-result cache. Only `classify` may
  create a missing cache directory.
- `--project-root` is the canonical local scope used in the cache key. It is not
  sent to the provider as a path.
- `--context-id` is a bounded local correlation label.
- `--request` reads the private request JSON described below.
- `--refresh` requests a new provider attempt instead of a fresh completed cache
  result. Its uncertain-attempt behavior is described under caching.

`preview` validates the request and renders the resolved provider (`"jev"` or
`"openai"`), model, deterministic cache key, and exact task summary. It performs
no network or vault access and does not create or mutate the registry or cache.
It is the only successful advice output allowed to include raw summary text.

`classify` is the only command that makes an inference request. `models`
retrieves catalog metadata for an enabled Jev tool; it does not classify or
create the advice cache.

## Request schema

The request is a private JSON object. Unknown and duplicate fields are denied.
The provider is resolved from the registry snapshot's tool adapter; there is no
provider override field in the request.

```json
{
  "schema_version": 1,
  "expected_registry_revision": 17,
  "tool_id": "jev-api",
  "model_id": "jev-api:jev-latest",
  "task_id": "example-task",
  "task_revision": 1,
  "task_summary": "A bounded operator-supplied task summary.",
  "baseline_difficulty": "easy"
}
```

For OpenAI Decisions, supply the explicit enabled `openai_decisions_api`
`tool_id` and owned supported Decisions `model_id` (for example,
`"tool_id": "openai-decisions"` and `"model_id": "openai-decisions:gpt-6-luna"`).

- `schema_version` is `1`.
- `expected_registry_revision` and `task_revision` are positive.
- Identifiers are nonempty and bounded.
- `task_summary` is at most 8192 UTF-8 bytes.
- The whole request is at most 16 KiB.
- `baseline_difficulty` is optional. When present it is exactly `easy`, `medium`,
  `hard`, or `extreme`, and is used only for comparison, never as the answer.

Requests that exceed these bounds, contain unknown or duplicate fields, or name a
difficulty outside the four levels are rejected with a sanitized error.

## Advisory results

The four difficulty levels are:

```text
easy
medium
hard
extreme
```

- For Jev (`jev_api`), the only accepted answer is a Choice over all four levels
  with probabilities and confidence. A different answer type is invalid. In
  particular, Noul is a separate yes/no probability; it is not abstention or
  confidence, and a Noul answer to this Choice request is rejected rather than
  interpreted as a difficulty.
- For OpenAI Decisions (`openai_decisions_api`), an accepted answer is either a
  Choice over the four levels with confidence (and validated probabilities when
  returned) or an explicit provider `refusal` answer. A refusal is returned as
  an advisory result with `difficulty: null`, `confidence: null`,
  `needs_review: true`, `reason_code: "refusal"`, and `agrees_with_baseline:
  null`.

A low-confidence Choice or OpenAI refusal remains advisory: it is returned with
`needs_review: true` and a review reason rather than being promoted, discarded,
or used to select a model or fallback provider. No answer changes a plan,
profile, difficulty, model, effort, permission, approval, host, worker, or Git
state.

## Successful classify output

`classify` returns bounded JSON with these fields:

- `untrusted_data: true`, `advisory_only: true`, and `mode: "shadow"`.
- `source` is `"provider"` or `"cache"`.
- Local correlation fields: `context_id`, `task_id`, `task_revision`, and
  `registry_revision` (plus `profile_id` when `--profile-id` was supplied).
- Selection fields: `tool_id`, `model_id`, and `requested_model`.
- `summary_sha256` is the digest of the normalized summary; the raw summary is
  not included.
- `cache_key` is the bounded local result key.
- `advice` is the existing validated `DecisionAdvice` object (with `provider`
  set to `"jev"` or `"openai"`, and `reason_code` set to `"classified"`,
  `"low_confidence"`, or, for OpenAI Decisions, `"refusal"`).
- `baseline_difficulty` is echoed when supplied, and `agrees_with_baseline`
  is boolean only when both the baseline and a valid classified difficulty are
  available (`null` when no baseline was supplied or on an OpenAI refusal).
- `provider_call_performed` reports whether this invocation made a provider
  request.
- `cache_age_ms` is present for a cache source; `elapsed_ms` is optional timing.

Field shapes are contractual. The values below are illustrative, not a recorded
provider response or a current-version claim:

```json
{
  "untrusted_data": true,
  "advisory_only": true,
  "mode": "shadow",
  "source": "provider",
  "context_id": "context-example",
  "task_id": "example-task",
  "task_revision": 1,
  "registry_revision": 17,
  "tool_id": "jev-api",
  "model_id": "jev-api:jev-latest",
  "requested_model": "jev-latest",
  "summary_sha256": "<64 lowercase hex characters>",
  "cache_key": "<bounded local cache key>",
  "advice": {
    "provider": "jev",
    "model": "jev-latest",
    "difficulty": "easy",
    "confidence": 0.87,
    "needs_review": false,
    "reason_code": "classified",
    "input_tokens": 120,
    "output_tokens": 4
  },
  "baseline_difficulty": "easy",
  "agrees_with_baseline": true,
  "provider_call_performed": true,
  "elapsed_ms": 850
}
```

The output excludes the raw task summary, API credential, and raw provider
response body. A cache source reports `provider_call_performed: false`,
`cache_age_ms`, and the usage recorded by the original provider result. A cache
hit performs no new provider request and no keychain read. Its historical usage
is not a new charge and must not be described as newly billed savings or as
proof that a second call was free.

Errors are sanitized bounded JSON with a stable code and a static message. Raw
request contents, HTTP diagnostics, vault diagnostics, credentials, and provider
bodies are not returned. A failure after a possible paid request must not be
blindly retried.

## Cache, expiry, and unknown attempts

The cache key binds the provider-specific versioned advice prompt/schema,
canonical local project scope, context ID, task ID and revision, normalized
summary digest, registry revision, resolved provider, and exact tool/model
argument (plus optional `profile_id` for profile-scoped Jev and OpenAI Decisions
calls). Changing any of these invalidates the result.

- Existing Jev version-1 prompt digests, cache keys, and persisted Jev records
  (including legacy records without a top-level `provider` field) retain their
  exact existing identities and remain readable and protected.
- OpenAI Decisions uses an OpenAI-specific prompt digest
  (`threadlane-openai-shadow-v1`), a provider domain-separated cache key, and an
  explicit `"provider": "openai"` field on all persisted records (`pending`,
  `uncertain`, and `completed`), plus `"profile_id"` when `--profile-id` was
  supplied.
- Cross-provider and cross-profile reuse of completed, pending, or uncertain
  records is prevented. Cached record and advice validation verifies the
  resolved provider, optional `profile_id`, model echo compatibility, and
  provider-valid result shape (allowing validated OpenAI refusals while
  rejecting Jev refusals or cross-provider/cross-profile records with
  `corrupt_cache`).
- The default completed-result lifetime is 3600 seconds; the lifetime applies
  only to completed results, and pending attempt markers are not reusable
  results.

The cache is under the existing private store hierarchy. When routed through
`threadlane-context`, the path is `plans_dir/advice-cache`; a direct caller
supplies `--cache-dir`. The directory must be same-UID `0700` under an existing
private parent, with `0600` regular non-symlink files and no symlink ancestors.
Unsafe existing paths, wrong ownership, FIFOs, symlinks, and oversized entries
are refused without chmod or repair. Reads are bounded and recheck file identity
before and after access. Writes are atomic and durable, enumeration is bounded to
at most 256 records and 1 MiB total across all providers, and a nonblocking
per-key lock refuses a busy key before any API effect. An existing result is
never overwritten unconditionally, and persisted entries and Debug output
contain no raw summary or API key. A short cache-wide lock serializes admission,
pruning, and publication; it is not held across credential or provider awaits.
Every publication checks fresh accounting, including the old record and its
temporary replacement. Refused new keys leave no additional lock files. Only
`classify` creates a missing cache directory.

Before a provider request can have an effect, `classify` seals a pending attempt
for its key. An abandoned pending attempt, or any timeout, transport failure, or
malformed response after a possible provider effect, remains uncertain. It does
not expire into an automatic replay and is never retried silently. A known
credential or request refusal before the API may clear its own new marker;
a failed refresh restores any previous completed, pending, or uncertain record.
After an uncertain outcome, review the attempt and any usage evidence, then use
an explicit `--refresh` only if a possible duplicate paid request is acceptable.

A completed entry past its TTL may be replaced by a new explicit `classify`
invocation. `--refresh` forces a new provider attempt even when a fresh completed
result exists, and is the only way to start a new attempt after an abandoned
pending or uncertain one. The command rechecks the registry revision, resolved
provider, and tool/model resolution after the provider response; if any changed,
the advice is not treated as current. No model selection or execution follows,
even for a confident answer.

## Context forwarding

The `advice` family is the fourth canonical `threadlane-context` sibling family.
Operational invocations validate the private version-1 context file and the live
`(binding_id, thread_uuid)` binding before spawning the sibling. The router
prepends exactly these arguments, in this order:

```text
--registry-dir CONTEXT.stores.registry_dir
--cache-dir CONTEXT.stores.plans_dir/advice-cache
--project-root CONTEXT.project_root
--context-id CONTEXT.thread_uuid
```

Then it forwards the inner arguments directly via argv:

```sh
threadlane-context --context /absolute/private/context.json \
  advice preview --request /absolute/private/request.json

threadlane-context --context /absolute/private/context.json \
  advice classify --request /absolute/private/request.json

threadlane-context --context /absolute/private/context.json \
  advice --cache-ttl-seconds 600 classify --request /absolute/private/request.json --refresh

threadlane-context --context /absolute/private/context.json \
  advice models --tool-id jev-api
```

`threadlane-context help advice` is offline: it resolves only the canonical
`threadlane-advice` sibling in the current executable's canonical parent
directory and passes exactly one `--help` argument. It reads no context or vault
and contacts no daemon. Operational forwarding likewise never starts a host or
supervisor or infers approval, and the same applies to outer `--help`,
context-free sibling help, and `--version` forms.

Inner overrides of the derived scope flags (`--registry-dir`, `--cache-dir`,
`--project-root`, and `--context-id`), other existing protected flags, and their
`=` and repeated forms are refused before the child starts. The context router
itself never creates the derived advice cache; only the sibling's explicit
`classify` may do so. It does not interpret `preview`, `models`, or `classify`,
and it adds no workflow, session, or flow semantics. See [native context
commands](NATIVE-CONTEXT.md) for the full router contract.

## Packaging

The `threadlane` Nix package builds the `threadlane-decisions` package alongside
the existing CLI packages, so the CLI output installs `threadlane-advice`. The
desktop package and its build flags are unchanged. From a checkout:

```sh
nix shell .#threadlane --command threadlane-advice --help
```

This adds no OS service, system package, global MCP registration, daemon, or
desktop autostart entry.

## Verification and live-test limits

On 2026-11-01, one explicit metadata-only CLI request successfully retrieved
`jev-latest` and `jev-preview` using a credential entered through Tools. The
registry digest and cache-directory existence were unchanged. This verifies
credential access and catalog retrieval on that Linux session, not classification
entitlement or correctness. No live Jev or OpenAI Decisions classification or
charge is claimed. The illustrative output above is not a recorded live
classification result; latency, cost, accuracy, low-confidence frequency,
refusal rate, and cached savings remain unmeasured.

Acceptance for this interface requires offline checks for: help and preview
(for both Jev and OpenAI Decisions, explicit and `--profile-id`) without
network, vault, or cache mutation; server-only Jev catalog contents and Jev-only
`models` scoping; profile-bound Jev and OpenAI Decisions exact-match routing
with no automatic provider selection or fallback; rejection of a wrong answer
type; four-level Choice parsing and OpenAI refusal handling; enabled
tool/model/revision/no-effort checks and mismatched tool/model/provider or
disabled/unavailable refusals; cache hits with no extra API or vault read;
cross-provider and cross-profile cache and pending/uncertain isolation with
preserved legacy Jev cache identities; invalidation by summary, scope, revision,
profile, provider, or model changes; concurrent single ownership; pending and
timeout refresh with no automatic retry; stale registry, profile, or adapter
drift after a call; keychain absence; symlink, FIFO, insecure, wrong-UID, and
oversize cache or request refusal; byte and count bounds; secret and summary
redaction; and side-effect-free baseline comparison with no plan, profile, or
model-routing mutation. Context fixtures in
[`crates/threadlane-native/tests/context_cli.rs`](../crates/threadlane-native/tests/context_cli.rs)
cover canonical argv order, protected override forms, offline advice help, legacy
families, and failures without calling external providers.

A future live acceptance requires separate human authorization, an unlocked OS
keychain credential configured through Tools for the target tool, an enabled
Jev or OpenAI Decisions tool with an explicit supported model, and one explicit
classification. A successful submission, mock-transport test, or package build
alone is not live provider evidence. Non-Unix private cache support may fail
closed, and keychain behavior on other platforms remains untested.
