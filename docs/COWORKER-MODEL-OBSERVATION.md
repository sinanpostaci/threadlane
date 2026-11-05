# Native Coworker Model Observation

## Configured vs. Observed Model Attribution

Threadlane separates native Codex chat configuration metadata from runtime-observed execution telemetry on `ObservationActor`:

- `model` and `effort`: Reserved exclusively for proven runtime-observed execution receipts. For a paired native Codex Coworker making MCP tool calls, these fields remain `None`.
- `selected_model` and `selected_effort`: Optional (`#[serde(default, skip_serializing_if = "Option::is_none")]`) sanitized metadata tokens carrying the model and reasoning effort configured on the verified native Codex thread (`thread/read` with `includeTurns: false`).

## Verification and Refresh Invariants

1. **Verified Exact-Thread Provenance**:
   - `AutomaticPairing::connect` carries `model` and `effort` into `VerifiedMcpObservationContext` only after `verify_thread_hint_candidate` corroborates the thread ID, canonical `cwd`, `project_id`, and `connection_identity` on the existing managed Linux desktop parent app-server.
   - Workflow profile assignments and caller-supplied MCP arguments are never used to populate `ObservationActor`.
   - `native_per_call_identity_verified` remains `false`.

2. **Bounded Per-Request Refresh**:
   - On new paired MCP requests, `PairedMcpServer` refreshes the cached `VerifiedMcpObservationContext` at most once per request through the existing verified managed parent WebSocket endpoint (`OBSERVATION_METADATA_REFRESH_TIMEOUT`, metadata-only `thread/read` with `includeTurns: false`).
   - Refreshed metadata is accepted only if the parent reconnect endpoint and PID match and the returned `NativeThread` matches the verified `thread.id`, loaded status (`idle` or `active`), canonical `cwd`, and `project_id`.
   - If the refresh fails, times out, or mismatches scope, `selected_model` and `selected_effort` are cleared (`None`) so stale selection is never reported as current, while normal MCP tool calls remain usable.

## Activity Feed Presentation

- When `selected_model` / `selected_effort` are present on a requester actor, the Activity feed formats them as `<model> · <effort> (selected)` (or `<model> (selected)`), appending to the requester line and timeline requester details while keeping runtime-observed `actorModel` / `actorEffort` as `Not reported` and leaving local MCP / worker executor rows untouched.
