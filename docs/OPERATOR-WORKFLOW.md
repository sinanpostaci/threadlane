# Trusted local operator workflow

`threadlane-operator` exposes the existing review and integration library as a
local CLI. Codex can use it as the trusted operator after checking the user's
authorized scope and the actual worker evidence. The desktop is not required
for these five operations. The Nix CLI package includes this executable.

This CLI does not create scope approvals, approve itself, expose new MCP tools,
or start providers. Explicit confirmation arguments prevent accidental subject
mix-ups; they are not authentication or a replacement for user authorization.
Worker text and JSON request data cannot grant permission to run the CLI.

## Commands

| Command | Request type | Successful output | Effect |
| --- | --- | --- | --- |
| `commit-task` | `CommitTaskChangesRequest` | `TaskCommitReport` | Commit validated changes in an owned task checkout |
| `inspect-review` | `InspectReviewSubjectRequest` | `ReviewSubject` | Read-only, freeze the current review subject |
| `accept-review` | `ReviewAcceptanceRequest` | `ReviewReceipt` | Persist an explicit, digest-bound acceptance |
| `propose-integration` | `IntegrationProposalRequest` | `IntegrationProposal` | Read-only, check the proposed integration |
| `integrate` | `ExecuteIntegrationRequest` | `IntegrationReceipt` | Integrate into an owned run integration checkout |

Request schemas are the public Rust types in
[`review.rs`](../crates/threadlane-operations/src/review.rs) and
[`integration.rs`](../crates/threadlane-operations/src/integration.rs).
Successful stdout contains the exact existing result DTO as JSON.

The following commands show the required arguments. Replace paths and
confirmations with values from the actual reviewed requests:

```sh
threadlane-operator commit-task --request /absolute/private/commit.json \
  --confirm-task TASK_WORKTREE_ID
threadlane-operator inspect-review --request /absolute/private/inspect.json
threadlane-operator accept-review --request /absolute/private/accept.json \
  --confirm-subject SUBJECT_SHA256
threadlane-operator propose-integration --request /absolute/private/propose.json
threadlane-operator integrate --request /absolute/private/integrate.json \
  --confirm-subject SUBJECT_SHA256 --confirm-target INTEGRATION_HEAD_SHA40
```

Confirmations must match the request before any library operation runs. Review
acceptance confirms both the expected digest and the embedded subject digest.
Integration confirms the subject and expected integration HEAD; the library
also validates current source/task/target HEADs, ownership and frozen receipts.

## Operator sequence

1. Inspect the completed worker report, exact model/effort evidence, write scope,
   and diff. A successful provider exit is not acceptance. Preserve failed or
   uncertain artifacts rather than replaying them.
2. Commit the validated task worktree. Run the required tests and independently
   review the actual committed patch. Test summaries in the request are trusted
   operator attestations; this CLI does not run those checks or prove reviewer
   model telemetry.
3. Inspect a review subject binding the base, task commit/tree, diff, scope,
   review policy and evidence. Accept that exact subject only after review.
4. Propose integration into a clean, owned integration worktree from the same
   run. Execute using the exact reviewed subject, receipt and expected HEADs.
   The primary source checkout remains unchanged.
5. Inspect the receipt and resulting tree. Publishing to main, pushing and
   deleting branches are separate authorized actions; this CLI exposes none
   of them.

A moved HEAD, changed evidence, stale subject, active job, dirty worktree or
invalid receipt can block the existing library operations. A confirmation flag
does not override any of those checks. Combining independent root runs and
automatic dependent-task dispatch remain unsupported by this checkpoint.

## Files and failures

Request paths must be absolute regular files, no larger than 1 MiB. On Unix,
files must have no group/other permission bits (normally `0600`). Symlink leaves
and ancestors, directories, invalid JSON, changing sizes and insecure modes are
refused. Reads and successful serialized output are bounded; the output limit
includes its trailing newline. The CLI never changes request permissions.

Errors use a bounded JSON code/message envelope on stderr and a nonzero exit.
Input failures use static `invalid_input` messages without the raw JSON or OS
error text. Mismatched confirmation is `subject_mismatch`, before operation
effects. Standard `--help` and syntax errors use normal Clap output.

If a mutation succeeds but its result cannot be serialized or written, the CLI
returns `outcome_unknown`. Inspect live worktrees and receipts before any retry;
there is no automatic retry. A missing stdout result is not proof that nothing
happened. Unix is the implementation target; Windows private-file and process
support remains unsupported, and macOS acceptance has not been performed.
