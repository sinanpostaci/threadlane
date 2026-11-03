# Coworker terms in plain language

This is a short glossary for the Coworker workflow. It explains the labels you may
see in conversation, Plans and Activity. A status records evidence and permission;
it does not create new authority by itself.

## Coworker and the five execution roles

- **Coworker** — your native Codex conversation. You choose its model and reasoning
  effort in Codex. Threadlane does not lock or override that choice, and Coworker has
  no profile role slot.
- **Orchestrator** — the read-only planning role. It turns your confirmed brief into
  a technical plan and task breakdown. It does not execute code.
- **Architect** — a read-only adviser for focused design or interface questions.
  Architect advice informs a plan; it is not approval to run work.
- **Coder** — the implementation role. It works only inside the approved task scope
  and its isolated worktree.
- **Reviewer** — reviews a Coder candidate against the approved task and returns a
  structured verdict, not just praise or prose.
- **Closure** — the single final review of the combined candidate produced by the
  whole job. Closure acceptance is not merge, push or install permission, and it does
  not replace required checks.
- **Legacy “follower”** — an older storage and record identifier kept so existing
  data still loads. The current human-facing name is Coworker; “follower” is not a
  sixth execution role or a profile assignment.

## Statuses you may see

- **Awaiting approval** — the plan is published and waiting for your explicit
  approval of its exact scope. Planning may read; approved execution has not begun.
- **Running** — a worker attempt is actively executing. This is activity, not proof
  of success.
- **Ready for review** — the Coder finished and passed the post-run diff gate. A
  candidate exists and needs structured review; it is **not** accepted yet.
- **Correct in scope** — this can follow a Reviewer verdict of Correct in scope, or a
  genuine failed frozen check when automatic check-failure corrections were explicitly
  enabled in the exact frozen approval. The corrective attempt stays inside the
  approved scope and budget; a requested scope change needs a fresh approval.
- **Held** — work is deliberately paused by an active hold, such as a pending brief
  amendment or conversation change, a dependency issue, or overlapping
  write/interface scope. Tasks with complete and disjoint declared dependency, write
  and interface scope may continue.
- **Blocked** — the current evidence does not allow progress: for example a blocked
  or unclear review verdict, or missing dependency proof. It needs a decision or fix,
  not repeated dispatch.
- **Unknown** — the last dispatch or mutation may have happened, but its result is
  unconfirmed. After an interruption, work may appear this way when completion cannot
  be verified; a still-running worker with verifiable liveness and receipts can remain
  **Running**. Reconcile existing receipts and state before dispatching again.
- **Completed** — on an individual tool, MCP or request call, this means only that the
  call finished or returned, not that its related job is complete. On a whole workflow
  job, Completed requires the actual checks, reviews and final Closure acceptance. It
  still does not mean the change was merged to main, pushed, installed or accepted on
  another platform.

## What the evidence means

- **Requested vs observed model and effort** — requested is recorded intent. Only a
  runtime or provider receipt shows what actually executed and at which effort.
- **Usage not reported** — a gap in evidence, not zero. Do not infer tokens from
  duration or copy another model’s numbers.
- **Delivered, acknowledged, read** — delivery is transport. An ACK confirms the
  recorded batch, not that a person read it or that a model explained it.
- **Coder completed** — output exists and may be ready for review. It is not approval,
  acceptance, integration or workflow completion.
- **Full workflow completed** — requires the actual checks, review receipts and final
  closure. A clean exit, a summary, or a merge/push/install is not a substitute.
- **Duplicate or missing events** — reconcile durable receipts and state. Do not
  blind-retry an unknown effect.

## Durable stops and honest limits

Threadlane persists workflow state across ordinary reopen and restart on tested Linux
paths. Interruption does not replay a provider request, create a new approval, or
resume an LLM at its last token. A stop or unknown outcome stops local assumptions of
progress; it cannot prove that remote inference has always terminated.

This glossary is terminology, not a release or readiness announcement. It does not
promise currently installed MCP tools, autonomous native wakeups, physical power-loss
durability, or macOS/Windows acceptance. Available tools and actions depend on your
installed build and explicit setup.

For operating steps, see [native operator commands](NATIVE-OPERATOR.md) and
[recovery](RECOVERY.md).
