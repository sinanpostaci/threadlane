# Reviewed dependency inputs

Codex remains the conversational decision maker. Threadlane prepares and runs
approved tasks with explicit, durable execution identities. A completed worker
is ready for review; completion alone does not release dependent tasks.

```text
Published plan + explicit scope approval
    |
    +-- Task A -- tests/review -- accepted commit -- integration receipt --+
    |                                                                   |
    +-- Task B -- tests/review -- accepted commit -- integration receipt --+
                                                                        |
                                               Explicit input preparation
                                                                        |
                                                    Frozen input commit
                                                                        |
                                               Explicit Task C dispatch
                                                                        |
                                                       Tests and review
```

## Operator sequence

Approve all required predecessor tasks, including transitive prerequisites.
Approval does not silently select additional tasks. Manual native routes are
not interchangeable with headless worker completion evidence.

Dispatch independent roots using the existing approved task interface. Inspect
the actual returned run and worker IDs, their requested/observed model evidence,
owned worktree references and scoped diff. Commit and review the actual patch
using the [trusted operator commands](OPERATOR-WORKFLOW.md). Accept the exact
review subject and explicitly integrate it into its own run integration area.

For each direct prerequisite of the child, select its logical task ID and the
stored review/integration receipt IDs. A private JSON selection file has this
shape:

```json
[
  {
    "task_id": "task-a",
    "review_receipt_id": "review-a",
    "integration_receipt_id": "integration-a"
  },
  {
    "task_id": "task-b",
    "review_receipt_id": "review-b",
    "integration_receipt_id": "integration-b"
  }
]
```

The IDs above are illustrative. Use only IDs returned by the real operations;
do not construct receipts or mark an uncertain worker successful.

The trusted local preparation command is:

```sh
threadlane-host prepare-dependencies \
  --registry-dir /absolute/private/registry \
  --plans-dir /absolute/private/plans \
  --authority-dir /absolute/private/authority \
  --sessions-dir /absolute/private/sessions \
  --approval-id APPROVAL_ID --task-id CHILD_TASK_ID \
  --predecessors /absolute/private/predecessors.json \
  --confirm-approval APPROVAL_SUBJECT_SHA256
```

This is an explicit local operator action, not a new MCP permission grant.
Preparation does not launch a provider. After a verified input is prepared,
Codex can dispatch the child through the same `(approval_id, task_id)` interface
used for roots. No model, prompt, base commit or write-scope override is accepted
through that interface.

## What is verified

Threadlane joins the logical predecessor task to its same-approval dispatch
record, actual runtime run/worker, owned task worktree, and persisted review and
integration proof. Owned worktree run/task tags are compact derived identifiers;
they are not the logical task ID or the actual runtime UUID.

Fan-in imports the reviewed **accepted task heads**. An integration target may
contain other history, so its entire tree is not automatically an authorized
input. Conflicting prerequisites block preparation. Git object/ref operations
do not merge or switch the primary checkout.

The prepared input fixes the original approval, child task/instruction,
prerequisite proofs, input commit/tree and exact seal-file bytes. Runtime
execution binds that input separately from the unchanged startup profile.
Only the execution base changes; model, effort, executable, budgets and file
permissions remain fixed. An originally exact file path cannot become a wider
directory permission merely because prerequisite code created that directory.

## Git decisions in Codex

Threadlane validates and executes typed Git operations; it does not choose a
merge strategy or expand its own authority. The follower can carry out routine
steps already covered by the approved plan. A changed main branch, merge/rebase
choice, or semantic conflict belongs to the orchestrator; a change to the user's
intended scope needs the user's decision. Native follower-to-orchestrator
consultation is still a product workflow to wire, not an automatic capability
provided by this dependency-input package.

## Recovery and observation

Status reads do not prepare inputs or dispatch workers. Prepared tasks can be
shown as `dependency_inputs_ready`; active/completed tasks retain their original
execution identity. Worker completion remains `ready_for_review`, with
`accepted: false`.

Dispatch intent is recorded before starting a worker. Missing confirmation of
the effect stays `Unknown`; retries use the original identity rather than
starting another worker. A changed input requires a new authorized plan
revision, not replacement of the input for an existing execution.

Cancellation and activity queries for an already-started child use its actual
run receipt. Later movement of a prerequisite does not change that child's
base or prevent asking it to stop. Keep prerequisite artifacts while they are
needed for verification; cleanup does not follow automatically from worker
exit. Main publication, push and release cleanup remain separate actions.

New readers preserve legacy root data. Old binaries are not promised support
for newly populated dependency fields or the new runtime schema. Legacy hosts
must not claim prepared-input execution support merely because a newer client
can display the new status.

See [validation](VALIDATION.md) for the accepted checkpoint and its remaining
live native, provider and platform gates. Synthetic provider fixtures do not
establish real provider effort telemetry or production multi-chat delivery.
