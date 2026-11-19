# Antigravity model-variant evidence

Contract `agy-1.5.12-catalog-v1` retains the 20 variants recorded through AGY 1.5.9. This
revision maps the four exact AGY 1.5.12 model variants (gemini-4-neon-low,
gemini-4-neon-medium, gemini-4-neon-high, and gemini-4-neon-xhigh) to low, medium, high, and
xhigh effort levels, respectively, while excluding unlabeled aliases such as
gemini-pro-orchestrator-neon.

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
