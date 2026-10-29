# Milestone 2A: bounded run history

This package implements the bounded-data prerequisite before durable real-worker
dispatch is connected to native Codex. See [the API contract](BOUNDED-DATA.md) for
request shapes, cursor rules and serialized byte accounting.

## Acceptance contract

- Current run and mutation results omit historical vectors and include bounded
  active-question detail. Replay returns the original accepted mutation snapshot.
- Run, revision and question history use binding-scoped ascending keyset pages.
  Window membership is pinned; documented mutable fields remain live views.
- SQL status counts and recent summaries do not materialize all history.
- Compact results stay within 64 KiB and history pages within 256 KiB, accounting
  for both structured and text copies in the MCP SDK. Existing event limits apply
  separately. A page byte boundary never skips its next item.
- Schema v1 upgrades transactionally to v2, processing legacy dedup records one
  at a time. V2 reopen skips migration; keys, hashes and original snapshots survive.
- Historical source records are preserved. Lifetime disk retention, unpaged
  trusted binding/wake-audit helpers and other platforms remain explicit limits.

## Validation

The primary agent ran 40 tests, formatting, Clippy, CLI smoke and independent
legacy-binary/paging probes on Linux/NixOS. See [validation evidence](VALIDATION.md).
From the repository root, reproduce with:

```sh
nix develop
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --locked
python3 scripts/smoke.py
```
