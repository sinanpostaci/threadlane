# Antigravity model-variant evidence

AGY 1.4.12 advertises concrete model variants such as
`gemini-4-argon-high` / `Gemini 4 Argon (High)` in `agy models`. Its structured
initialization event reports the selected concrete model, but omits a separate
reasoning effort field. Threadlane recognizes only the 13 exact catalog IDs in
`workers::antigravity_models` under contract `agy-1.4.12-catalog-v1`.

For the Antigravity adapter, a valid structured `event: init` with a matching
`init.model` can establish the encoded variant. The sealed report records
`evidence.effort_evidence_source: antigravity_catalog_model_variant` separately
from `direct_stream_metadata`. This is CLI-reported model selection; it does not
independently attest a cloud reasoning budget. Requested arguments, arbitrary
`*-high` suffixes and tool output never supply this evidence. Explicit or later
conflicting model/effort metadata remains a verification failure. Unknown model
IDs still require their own explicit provider evidence. The generic stream
parser and other providers do not enable the AGY catalog rule.

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
Missing results, provider errors, cancelled
or timed-out runs and stale scope never become successful from the catalog rule.
