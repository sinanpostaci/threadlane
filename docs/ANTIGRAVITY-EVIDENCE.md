# Antigravity model-variant evidence

Canonical AGY 1.5.9 (`agy models`, read without inference on 2026-11-17)
advertises 20 concrete effort-encoded model variants: the 16 exact identifiers
previously recorded in AGY 1.5.3 (`agy-1.5.3-catalog-v1`, preserving the 13 AGY
1.4.12 identifiers such as `gemini-4-argon-high` / `Gemini 4 Argon (High)` plus
`gemini-4-carbon-low`, `gemini-4-carbon-medium`, and `gemini-4-carbon-high`)
plus `gemini-4-hydrogen-low` (`Low`), `gemini-4-hydrogen-medium` (`Medium`),
`gemini-4-hydrogen-high` (`High`), and `gemini-4-hydrogen-xhigh` (`XHigh`). Its
structured initialization event reports the selected concrete model, but omits a
separate reasoning effort field. Threadlane recognizes only those 20 exact
catalog IDs in `workers::antigravity_models` under contract
`agy-1.5.9-catalog-v1` (observed CLI `1.5.9`). `agy models` also lists
`gemini-pro-orchestrator` and `gemini-pro-orchestrator-argon` without encoded
effort labels; Threadlane does not register them in the effort catalog or
derive effort evidence from them.

For the Antigravity adapter, a valid structured `event: init` with a matching
`init.model` can establish the encoded variant. The sealed report records
`evidence.effort_evidence_source: antigravity_catalog_model_variant` separately
from `direct_stream_metadata`. This is CLI-reported model-variant selection; it
does not independently attest a cloud reasoning budget. Requested arguments,
arbitrary `*-high` / `*-xhigh` suffixes, aliases, case variants and tool output
never supply this evidence. Explicit or later conflicting model/effort metadata
remains a verification failure. Unknown model IDs still require their own
explicit provider evidence. The generic stream parser and other providers do not
enable the AGY catalog rule.

The canonical worker target requires an explicit effort and rejects a known
concrete variant paired with a different effort. Catalog imports already expose
a matching singleton effort; the profile editor preserves the concrete model ID
and saves that explicit effort. Existing empty-effort catalog records remain
readable. Refresh their catalog metadata and reselect/resave the same effort, or
perform an explicitly authorized, backed-up registry migration. Do not switch
models or alter other role assignments during migration.

Completion, verified model/variant, scoped diff checks, review acceptance and
integration remain separate gates. A provider `SUCCESS` result with
`finished_reason: print_timeout` is an incomplete run and fails the result gate.
Missing results, provider errors, cancelled or timed-out runs and stale scope
never become successful from the catalog rule, and catalog updates never
retroactively alter prior failed run or job statuses.
