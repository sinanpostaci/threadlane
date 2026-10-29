# M4 configuration and observation checkpoint

This is an intermediate validated checkpoint, not a completed production workflow.
Native Codex remains the conversational interface. Threadlane does not listen to
conversation transcripts or treat model output as operator approval.

## Available in this checkpoint

- Global tool/model registry with revision checks, explicit CLI discovery, API
  credential controls, and exact model/effort choices.
- Workflow profile creation, editing, cloning, deletion, defaults, and difficulty
  overrides. Updating a profile does not silently alter a running worker.
- Revisioned plan drafts, explicit publication, and separate delivery receipts.
- Read-only scoped activity history with filters and cursor pagination. Viewing
  history never consumes follower delivery or acknowledgement cursors.
- Native pairing bootstrap and an operator probe that refuses shared or
  cross-wired chat connections. Fixture success does not establish live isolation.
- Owned Git integration/cleanup operations, optional tool capabilities, and
  reproducible CLI/Linux desktop package expressions.
- Scoped OpenCode V2 worker configuration with exact model pins, blocked nested
  delegation, and no silent provider fallback. The installed CLI accepted the
  generated configuration without an inference request.

## Recorded validation

The integrated Rust checkpoint passed 166 tests and workspace/all-target Clippy
with warnings denied. The frontend passed production compilation and 27 unique
tests, including eight history tests. Python tooling passed all 23 tests.
Formatting-only line wrapping was applied after that Rust run.

The Nix CLI source build passed. The Linux desktop source build passed after
correcting Cargo's build subdirectory, and the resulting wrapped executable
passed `--help`. These package builds establish the packaging path at the tested
source snapshots; subsequent workflow execution changes need their own checks.

Browser acceptance at 390 pixels covered creating a synthetic tool/profile,
history filtering, event detail expansion, and a corrected history layout with
no horizontal overflow. These checks used the explicit `?preview=1` DEV fixture.
The earlier native Wayland rendering check belongs to the previous milestone.

## Work continuing separately

The next slice connects binding-scoped profile/plan MCP operations to immutable
operator-approved execution artifacts and a separately owned plan-host process.
The host must outlive desktop closure. Dispatch must load the approved instruction,
model, scope, and idempotency key; a caller cannot replace them.

Dependent tasks remain blocked until their input commits are verified. A worker
reporting completion means ready for review, not accepted, integrated, or applied.
Codex roles remain native/manual routes; current headless providers are AGY and
OpenCode. Automatic merge/push is not implied by worker completion.

## Acceptance still requiring evidence

- Two distinct live native Codex chats with verified per-call routing.
- Native MCP refresh/restart and busy-turn wake-up delivery.
- Real product API credentials and provider calls, initiated explicitly.
- Current native desktop interaction with the complete workflow execution slice.
- macOS/Windows packaging and runtime acceptance. The manual CI matrix has not
  been dispatched; Windows lifecycle/private-file support is not established.

No live-chat creation, paid product API tests, persistent OS services, global
configuration changes, or NixOS activation were performed for this checkpoint.
