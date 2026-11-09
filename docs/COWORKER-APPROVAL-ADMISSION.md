# Trusted-local Coworker approval admission

The `flow coworker-admission` commands admit an existing Coworker job from its
verified published planning result and exact existing authority approval. They
preview the complete consent subject, record immutable linkage and a frozen
controller execution envelope, and expose explicit persistence reconciliation.
Recording admission starts no execution. `start_execution` is a separate action.

This is a trusted-local operator route, not an MCP self-approval tool. Native
Codex remains the human-selected conversational Coworker with no execution model
slot. Saved profiles, catalog entries, planning grants and worker prose grant no
code execution authority. Local attestation is not cryptographic human identity;
`native_per_call_identity_verified` remains `false`.

## Supported order and consent

1. Produce the actual structured Orchestrator plan under a separately approved
   planning grant. Resolve the work brief's execution-approval blockers through
   the supported planning/brief records; publication alone grants no authority.
2. Obtain the exact authority approval for that published plan. Use
   `approve prepare`, present its concrete Worker task scope, canonical repository,
   base commit, model/effort/executable pins, budgets and concurrency to the human,
   then `approve record` only after that exact authority scope is approved. Use
   the returned approval ID; admission does not create authority approvals.
3. Build one private `AdmissionRequest` from that job's original job/ledger
   revisions and recorded `planning_result`. Run `flow coworker-admission preview`
   before recording admission consent. Show the complete preview to the human,
   including every role and execution power listed below. The existing authority
   record alone does not authorize Reviewer, Closure, checks, corrections or Git.
4. After the human confirms the complete unchanged scope, record the same request
   with `--operator-approved` and all three returned digests. If that identical
   complete scope was already explicitly approved, do not request redundant
   consent. Worker-only approval, a generic task request, or a saved profile does
   not satisfy this condition. Changed or expanded scope needs a new preview and
   fresh consent before recording; never silently refresh confirmed inputs.
5. Inspect admission persistence, then call the read-only MCP
   `preview_execution {"job_id":"ACTUAL_JOB_ID"}` to inspect current readiness.
   Historical admission integrity and current execution eligibility are separate.
6. Only when current authority/readiness permits, separately call
   `start_execution` with the returned frozen `envelope_digest` as
   `expected_envelope_digest`. Recording, inspection and reconciliation do not
   start a host, provider, check, worktree, commit, integration or Closure.

Present all of these, without reducing consent to a model name or a plan summary:

- The native Coworker is human selected. Separately identify configured
  Orchestrator/Architect tiers (including unavailable or disabled choices), the
  existing planning grant's policy, expiry, call budgets and reservations, and
  proven historical Orchestrator/Architect references. Configured roles are not
  proof they ran and do not receive new authority through admission.
- Every Worker (`coder`) task's exact tool/adapter/model/effort, difficulty,
  canonical executable, task/dependency/interface metadata and write paths; the
  canonical repository, immutable base commit/tree, work brief, published plan,
  profile and registry revision; authority `max_parallel` and stream/runtime budgets.
- The exact Reviewer and Closure choices and canonical executables, selected
  difficulty/default resolution, each role's call/runtime/timeout/output/structured
  response budgets, and all task-attempt and correction caps.
- Each frozen check's ID, kind, canonical executable, exact argument vector,
  repository-relative working directory, timeout and stdout/stderr caps, and
  whether it is required before task review and/or candidate Closure. No shell
  string is substituted for a frozen executable and argument vector.
- Explicit check-failure correction opt-in (default off), any independence
  completeness receipt and shadow advisor policy, task-worktree commit power,
  local RunIntegration power and integration strategy. These powers do not imply
  permission to merge/push the user's primary branch or perform cleanup.
- The exact `preview_digest`, `approval_subject_digest`, `envelope.envelope_digest`,
  original source revisions, proposed resulting revisions, stable request key
  and original creation timestamp.

## Private context commands

Use the actual `operator_context.context_router_executable` and `context_path`
returned for this chat, or the trusted operator's supplied private context.
The following paths and uppercase tokens are illustrative, not live authority:

```sh
# Obtain the existing authority approval first, after exact scope approval.
threadlane-context --context /private/context.json approve prepare \
  --request /private/approval-intent.json
# Save the authority preview's request unchanged as /private/approval-request.json.
threadlane-context --context /private/context.json approve record \
  --request /private/approval-request.json \
  --confirm-subject AUTHORITY_SUBJECT_SHA256 --confirm-thread ACTUAL_THREAD_UUID \
  --operator-approved

# Preview complete admission before recording its consent.
threadlane-context --context /private/context.json flow coworker-admission preview \
  --request /private/admission.json

# Only after confirmation of that complete unchanged preview:
threadlane-context --context /private/context.json flow coworker-admission record \
  --request /private/admission.json --operator-approved \
  --confirm-preview PREVIEW_SHA256 --confirm-approval APPROVAL_SUBJECT_SHA256 \
  --confirm-envelope ENVELOPE_SHA256

threadlane-context --context /private/context.json flow coworker-admission inspect \
  --job-id ACTUAL_JOB_ID

# Only when persistence recovery is required, using the ORIGINAL persisted key:
threadlane-context --context /private/context.json flow coworker-admission reconcile \
  --job-id ACTUAL_JOB_ID --request-key ORIGINAL_ADMISSION_REQUEST_KEY
```

`threadlane-flow` has no `--context` flag. Direct callers use its five existing
canonical private store flags (`--state-dir`, `--registry-dir`, `--plans-dir`,
`--authority-dir`, `--sessions-dir`) plus fixed `--binding` and `--thread`.
These remain global flags accepted before or after the subcommand. The context
router supplies the locked stores and chat scope; do not override or reconstruct
another binding. Live supervisor scope verification precedes request/store
operations. Both nested binding/thread/job tuples must match the fixed scope.

Request files must be absolute, regular, non-symlink private `0600` files, bounded
to 1 MiB; store directories are existing canonical private `0700` directories.
Keep context, requests, previews and receipts outside source control. Success
stdout is bounded to 1 MiB and wrapped in `untrusted_data: true`,
`route: "trusted_local_operator"`, `native_per_call_identity_verified: false`,
`binding_id` and `data`. None of these fields authenticates human intent.

## AdmissionRequest wire example

This synthetic one-task JSON illustrates the actual nested request type. It is
not a valid live approval: IDs, digests, model names, paths and timestamps below
are invented, and the repeated-character digests were not computed from these
objects. Never submit it as authority or create runtime records to match it.
Copy actual recorded planning/authority subjects and exact live profile choices.
`planning_result` is the full stored reference, not a plan ID supplied by the
operator. Its policy digest describes the separate planning grant; it need not
be the execution policy digest. Optional resolved-question/deferred-proposal
fields, when present on the actual reference, must also be preserved.

Both `expected_job_revision` values are the original source revision `7`, and
`binding.expected_ledger_revision` is the original ledger revision `20`. For a
new linkage, the preview might propose job revision `8` and ledger revision `21`;
read the actual preview rather than calculating these values yourself. Admission
internally prepares the envelope against the prospective linked job. Do not
rewrite the request's source revisions to the resulting revisions before record.

`execution.created_at_ms: 1700000100000` and `request_key: "admit_example_001"`
remain unchanged across preview, confirmation, duplicate record and recovery.
`planning_result.request_key` is a different, historical planning-call key.
`null` role difficulty selects the profile's explicit default, not an arbitrary
tier or model fallback. The example disables corrections and shadow advice;
finite numbers here illustrate fields, not a new runtime policy.

```json
{
  "request_key": "admit_example_001",
  "binding": {
    "binding_id": "bnd_0196ed00-0000-7000-8000-000000000703",
    "thread_uuid": "0196ed00-0000-7000-8000-000000000703",
    "job_id": "job_example",
    "expected_job_revision": 7,
    "expected_ledger_revision": 20,
    "approval_id": "apr_77777777777777777777777777777777",
    "policy": {
      "schema_version": 1,
      "policy_id": "execution-example",
      "allow_rules": [
        {
          "tool_id": "example-cli",
          "model_id": "example-model",
          "efforts": [
            "high"
          ],
          "allow_no_effort": false,
          "roles": [
            "coder",
            "reviewer",
            "closure"
          ]
        }
      ]
    },
    "controller_envelope": {
      "allowed_reviewer_choices": [
        {
          "tool_id": "example-cli",
          "adapter": "opencode_v2",
          "route": "headless_worker",
          "model_id": "example-model",
          "model_arg": "example/provider-model",
          "effort": "high",
          "matched_difficulty": null
        }
      ],
      "allowed_closure_choices": [
        {
          "tool_id": "example-cli",
          "adapter": "opencode_v2",
          "route": "headless_worker",
          "model_id": "example-model",
          "model_arg": "example/provider-model",
          "effort": "high",
          "matched_difficulty": null
        }
      ],
      "max_in_scope_correction_attempts": 0,
      "allowed_check_ids": [
        "cargo_test"
      ],
      "local_git_integration_allowed": true,
      "allow_check_failure_corrections": false
    },
    "task_interfaces": {
      "task_example": {
        "owned_interfaces": [
          "example-interface"
        ],
        "consumed_interfaces": []
      }
    }
  },
  "planning_result": {
    "grant_id": "grant_example",
    "reservation_id": "reservation_example",
    "request_key": "plan_example_001",
    "run_id": "run_example",
    "report_artifact_id": "report_example",
    "output_digest": "1111111111111111111111111111111111111111111111111111111111111111",
    "work_brief_digest": "2222222222222222222222222222222222222222222222222222222222222222",
    "project_base": {
      "repository_path": "/private/example/repository",
      "base_commit": "3333333333333333333333333333333333333333",
      "base_tree_sha": "4444444444444444444444444444444444444444"
    },
    "profile_id": "profile_example",
    "registry_revision": 12,
    "policy_digest": "5555555555555555555555555555555555555555555555555555555555555555",
    "selected_choice": {
      "tool_id": "example-cli",
      "adapter": "opencode_v2",
      "route": "headless_worker",
      "model_id": "example-model",
      "model_arg": "example/provider-model",
      "effort": "high",
      "matched_difficulty": null
    },
    "plan_id": "plan_example",
    "plan_revision": 2,
    "plan_digest": "6666666666666666666666666666666666666666666666666666666666666666",
    "plan_status": "published",
    "task_interfaces": {
      "task_example": {
        "owned_interfaces": [
          "example-interface"
        ],
        "consumed_interfaces": []
      }
    },
    "architect_advice_ids": [],
    "recorded_at_ms": 1700000000000
  },
  "work_brief_digest": "2222222222222222222222222222222222222222222222222222222222222222",
  "execution": {
    "binding_id": "bnd_0196ed00-0000-7000-8000-000000000703",
    "thread_uuid": "0196ed00-0000-7000-8000-000000000703",
    "job_id": "job_example",
    "expected_job_revision": 7,
    "policy": {
      "schema_version": 1,
      "policy_id": "execution-example",
      "allow_rules": [
        {
          "tool_id": "example-cli",
          "model_id": "example-model",
          "efforts": [
            "high"
          ],
          "allow_no_effort": false,
          "roles": [
            "coder",
            "reviewer",
            "closure"
          ]
        }
      ]
    },
    "reviewer_choice": {
      "tool_id": "example-cli",
      "adapter": "opencode_v2",
      "route": "headless_worker",
      "model_id": "example-model",
      "model_arg": "example/provider-model",
      "effort": "high",
      "matched_difficulty": null
    },
    "closure_choice": {
      "tool_id": "example-cli",
      "adapter": "opencode_v2",
      "route": "headless_worker",
      "model_id": "example-model",
      "model_arg": "example/provider-model",
      "effort": "high",
      "matched_difficulty": null
    },
    "role_budgets": {
      "coder": {
        "role": "coder",
        "max_total_calls": 1,
        "max_total_runtime_ms": 300000,
        "max_timeout_ms_per_call": 300000,
        "max_stdout_bytes_per_call": 1048576,
        "max_stderr_bytes_per_call": 1048576,
        "max_structured_response_bytes": 65536
      },
      "reviewer": {
        "role": "reviewer",
        "max_total_calls": 1,
        "max_total_runtime_ms": 300000,
        "max_timeout_ms_per_call": 300000,
        "max_stdout_bytes_per_call": 1048576,
        "max_stderr_bytes_per_call": 1048576,
        "max_structured_response_bytes": 65536
      },
      "closure": {
        "role": "closure",
        "max_total_calls": 1,
        "max_total_runtime_ms": 300000,
        "max_timeout_ms_per_call": 300000,
        "max_stdout_bytes_per_call": 1048576,
        "max_stderr_bytes_per_call": 1048576,
        "max_structured_response_bytes": 65536
      }
    },
    "max_in_scope_corrections_per_task": 0,
    "max_total_corrections_per_job": 0,
    "max_total_task_attempts_per_job": 1,
    "check_commands": {
      "cargo_test": {
        "check_id": "cargo_test",
        "kind": "tests",
        "executable": "/private/example/toolchain/bin/cargo",
        "argv": [
          "test",
          "--workspace"
        ],
        "repo_relative_cwd": ".",
        "timeout_ms": 300000,
        "max_stdout_bytes": 1048576,
        "max_stderr_bytes": 1048576,
        "required_for_task_review": true,
        "required_for_candidate_closure": true
      }
    },
    "allow_task_worktree_commits": true,
    "allow_run_integration_execution": true,
    "default_integration_strategy": "merge_no_ff",
    "independence_completeness_receipt": null,
    "shadow_jev_policy": null,
    "created_at_ms": 1700000100000,
    "operator_attested": false,
    "confirm_approval_subject_digest": null,
    "confirm_envelope_digest": null
  },
  "reviewer_difficulty": null,
  "closure_difficulty": null
}
```

The schema rejects unknown fields. There is no top-level JSON preview confirmation
or self-approval field. Preview normalizes `execution.operator_attested` to
`false` and its two confirmation fields to `null` for subject identity. Record
requires the explicit CLI flag and **all three** confirmation flags even if JSON
contains `operator_attested: true`. Non-empty JSON approval/envelope confirmation
digests that disagree with the flags are rejected. CLI flags attest only the
human decision already made; flags do not create that decision.

## Preview and result shapes

Preview returns a full `AdmissionPreview` at `data`, with these fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | Admission preview schema `1`. |
| `request` | Complete normalized original `AdmissionRequest`, including original source revisions and timestamp. |
| `resulting_ledger_revision` | Prospective ledger revision including the linkage/marker transaction. |
| `proposed_job` | Complete prospective linked job, resolved tasks, dependencies, interface ownership and frozen capabilities. |
| `approval_subject_digest`, `max_parallel`, `authority_budgets` | Exact existing authority subject, concurrency and Worker budgets. |
| `envelope` | Full prospective frozen controller execution envelope; `operator_attested` is `false` in the preview. |
| `executable_pins` | `workers` keyed by task ID, plus `reviewer` and `closure`; each contains `choice` and `canonical_executable`. |
| `planning_configuration` | Preflight of every configured Orchestrator/Architect tier under the separate planning policy, with missing-role/blocked reasons explicit. |
| `planning_profile`, `planning_models`, `planning_tools` | Actual configuration, including disabled tools/assignments; this is separate from historical execution proof. |
| `planning_authority` | Existing ledger-validated planning grant with original policy, budgets and reservations. |
| `coworker_configuration` | `human_selected_native_conversation_no_profile_execution_slot`. |
| `preview_digest` | Domain-separated SHA-256 covering every other preview field, including planning display configuration and canonical pins. |

The three record flags come from `data.preview_digest`,
`data.approval_subject_digest` and `data.envelope.envelope_digest` respectively.
They are distinct subjects and cannot be substituted for one another. Live
record recomputes consent before persistence; changed revisions, choices,
canonical executable paths, scopes, checks, budgets, timestamps or other preview
subjects cause a refusal instead of silently adopting new values. Canonical path
pins do not claim cryptographic measurement of executable file contents.

Successful record and reconcile return the same compact shape. This synthetic
response illustrates a newly linked job; its digests are placeholders, not
confirmation values:

```json
{
  "untrusted_data": true,
  "route": "trusted_local_operator",
  "native_per_call_identity_verified": false,
  "binding_id": "bnd_0196ed00-0000-7000-8000-000000000703",
  "data": {
    "request_key": "admit_example_001",
    "job_id": "job_example",
    "job_revision": 8,
    "request_digest": "8888888888888888888888888888888888888888888888888888888888888888",
    "preview_digest": "9999999999999999999999999999999999999999999999999999999999999999",
    "approval_subject_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "envelope_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "stage": "complete"
  }
}
```

`data.job_revision` is the original admission's **resulting linked job revision**,
not source revision `7` and not a later live revision. An identical completed
duplicate returns the original result even after job progress. The normalized
`request_digest` is separate from the complete `preview_digest`.

Inspect returns `AdmissionInspection` at `data`: `state`, `intent_present`,
`linkage_present`, `runtime_present`, and `receipt` (null if absent). A present
receipt contains exactly `schema_version`, the original full `preview`, the
original `confirmation` (`trusted_local_attested`, `preview_digest`,
`approval_subject_digest`, `envelope_digest`), `stage` and `runtime_initialized`.
The original creation timestamp is therefore available at
`data.receipt.preview.request.execution.created_at_ms`, and the original source
and resulting revisions remain separately inspectable in that preview.

After readiness inspection, the separate MCP requests have these shapes; replace
tokens with the actual returned values, never these placeholders:

```json
{"job_id":"ACTUAL_JOB_ID"}
```

Use the above for `preview_execution`, then for `start_execution`:

```json
{"job_id":"ACTUAL_JOB_ID","expected_envelope_digest":"RETURNED_ENVELOPE_SHA256"}
```

`preview_execution` reports `needs_approval`, `missing_execution_envelope`, or
`approved_and_frozen`. It is process/provider-free and redacts prompts and full
host executable paths; the trusted-local admission preview is the complete
consent view. Readiness is a current validation result, not consent and not an
execution receipt. Start still rechecks exact scope, planning, live dependencies,
the frozen digest and complete-admission persistence before opening the driver,
setting up notices or launching an owner. Current frozen authorization can drive
Workers, checks, Reviewer, bounded corrections, local integration and one accepted
Closure. `AllTasksIntegrated` alone is not completion.

## Persistence, inspection and explicit reconciliation

Admission spans separate durable stores, not one transaction over all files:
original consent intent first; workflow linkage and immutable marker in one
transaction; frozen controller runtime; then completion receipt. The receipt's
stored `stage` uses `intent`, `linked`, `runtime_recorded`, `complete`, or
`uncertain`. `runtime_initialized` is monotonic evidence of successful runtime
persistence, not evidence a provider ran. Missing an already initialized runtime
must never reset counters by constructing a fresh one.

Pure `preview` and `inspect` take no persistence locks, sync no files, repair
nothing and create no ledger, lockfile or observation journal. All four admission
commands bypass observation journaling; the private intent/receipt is the durable
mutation record. An inspection classifies independent subjects, not just the last
stored stage:

| Inspection `state` | Meaning and next action |
| --- | --- |
| `absent` | No admission intent/marker exists. May be an unadmitted job or an older valid manual job; absence itself grants nothing. |
| `partially_recorded` | Valid intent with missing writes. Preserve the original key and consent; explicit reconcile is required before execution. |
| `complete` | Matching linkage/runtime, stored Complete receipt and a successful durability witness in this process. Current execution eligibility still requires its own checks. |
| `uncertain` | Either stored `uncertain`, or a stored `complete` receipt without this process's durability witness. Inspect `receipt.stage` to distinguish them; inspection never syncs to resolve either. |
| `conflicting` | Independent subjects disagree, or a required receipt/runtime disappeared. Stop advancement; reconcile cannot overwrite a conflict. Corrupt/unreadable receipts return an error rather than repair. |

A fresh CLI process can inspect an unchanged valid `receipt.stage: "complete"`
as `state: "uncertain"`; this is not proof of an incomplete admission. At deliberate
effectful boundaries (`start_execution`, owner launch/ensure, code-driver reconcile
and controller effect guards), the service may take the per-job admission lock,
validate the unchanged stored Complete receipt with `runtime_initialized: true`
and matching linkage/marker/runtime envelope, sync the **existing** workflow
ledger, controller ledger, receipt and plans directory, then reread and validate
before caching a process-local witness. Only successful sync returns establish
the witness. File bytes/mtime, revisions, consent, counters, leases and provider
state do not change during this durability establishment; no missing admission
write is performed. Pure preview/status/inspect/cancellation do not establish it.
A readiness preview can therefore be `approved_and_frozen` for an unchanged stored
Complete receipt even though a fresh pure admission inspection is `uncertain`.

Stored `uncertain`, `intent`, `linked` and `runtime_recorded` receipts remain
fenced, as do missing initialized runtime, missing mandatory receipts and
conflicts. **Start never finishes an incomplete admission.** Explicit
`coworker-admission reconcile --job-id ... --request-key ORIGINAL_KEY` reuses only
the persisted original consent and subjects, completes permitted missing writes
or reports drift/conflict. It cannot replace a key, refresh consent, change the
creation timestamp, widen scope or reset an initialized runtime. Complete
duplicate record and Complete reconcile sync existing persistence and return the
original receipt without rewriting it. Reconciliation itself runs no execution.

Known failures before an intent exists retain their sanitized refusal code.
After an intent exists, non-durability failures report
`admission_reconciliation_required`; a persistence uncertainty reports
`persistence_uncertain`. An uninspectable mutation result or failed bounded
stdout/flush reports `outcome_unknown`. The admission may already be recorded.
Inspect the original job and reconcile its original key before considering any
retry; do not generate a replacement request to evade uncertainty or introduce a
provider retry. A failed command/response is not proof that nothing persisted.

## Immutable conflicts and older routes

`approval_linkage` is immutable, as are the marker and frozen execution subject.
An existing Worker-only job cannot gain Reviewer/Closure, checks, correction or
Git powers by editing its approval, choosing a larger envelope, or reusing its
job ID with another key. A different plan, authority, base, policy, write scope,
capability envelope or execution envelope requires the supported new-job or
amendment/replan path: preserve the old records, produce the appropriate published
planning result, obtain exact authority and complete-scope consent, and admit the
new job. A superseded old job remains historical; inspecting its receipt after
supersession does not make it executable again.

An already manually linked job can opt in only if the service proves its exact
immutable task/linkage/capability subjects and any existing runtime already match;
this is no permission to retrofit a different subject. Without admission intent
or marker, older valid manual jobs retain their existing binding/envelope and
provider resolution behavior. `coworker-execution preview|record|inspect` remains
the separate manual envelope route, not the automatic complete admission route.
Historical/legacy scopes may expose only diagnostic Phase 1 behavior and are not
silently upgraded. For newly admitted jobs, Workers, Reviewer and Closure consume
the canonical executable pins: a marked job cannot fall back to PATH when a pin
is missing, changed or unavailable. Legacy/manual resolution applies only to jobs
without admission intent/marker.

## Evidence and follow-up boundaries

Admission, CLI and controller regression sources use disposable fixtures and fake
providers/processes. They cover pure preparation/inspection, exact confirmations,
stale revisions, duplicate/concurrent requests, persistence boundaries, original-key
recovery, missing/conflicting records, cross-process completion witnesses and a
full detached-owner DAG. Their presence is not a claim that the current combined
gates have passed. Formatting, combined behavioral tests and Clippy must be
completed in the combined checkout. No live admission, job or provider was created
for this package.

Fixtures do not establish installed-package, live-provider, original native-chat
delivery, forced live-provider interruption or physical power-loss acceptance.
Startup ergonomics, larger runtime limits, decomposition rules, user-language role
outputs (with English code/comments), native UI status and observation performance
remain follow-up product work. Generic conversation-change/dispatch races and the
missing-check-executable status/stop issue are not fixed by admission documentation.
