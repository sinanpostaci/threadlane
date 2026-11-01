# Direct user-message steer notifications

This explicit opt-in delivery mode (`delivery: "user_message_steer"`) on
`NativeToolOutputConfig` delivers the fixed approved-session wake-up notice
(`ApprovedSessionWakeupContext::wakeup_message`) over the existing private Unix
WebSocket bridge without spawning `codex queue`, starting a second backend, or
falling back to `toolOutput`.

## Operator configuration

Set `"delivery": "user_message_steer"` in the private (`0600`) native context JSON
file passed to `--notify-tool-output-context`:

```json
{
  "schema_version": 1,
  "socket_path": "/private/runtime/native-codex/control.sock",
  "thread_id": "01900000-0000-7000-8000-000000000001",
  "project_root": "/canonical/path/to/repository",
  "model": "gpt-6-luna",
  "effort": "max",
  "delivery": "user_message_steer"
}
```

Omitting `delivery` preserves `NativeNotificationDelivery::ToolOutput`, legacy JSON
serialization, and the legacy `NativeToolOutputConfig::digest()` value. Selecting
`user_message_steer` changes the canonical digest and records
`transport: "unix_user_message_steer"` in `HostNotificationSummary`.

## Routing and safety invariants

1. **Pre-effect validation**: Validates endpoint/peer/process ancestry (when
   `reconnect` is configured), `initialize` (with `experimentalApi: true`),
   `initialized`, `thread/read` (`includeTurns: false`), and `thread/resume`
   (`excludeTurns: true`) against the frozen `thread_id`, `project_root`, `model`,
   and `effort`.
2. **Idle thread (`status.type == "idle"`)**: Sets `may_have_written = true`
   immediately before sending `turn/start` with
   `input: [{"type": "text", "text": <fixed_wakeup_message>}]` and no `toolOutput`
   or model/effort overrides, then validates the returned `turn.id` UUID.
3. **Busy thread (`status.type == "active"`)**: Queries `thread/turns/list` with
   bounded read-only parameters
   `{"threadId": <thread_id>, "limit": 1, "sortDirection": "desc", "itemsView": "summary"}`.
   Requires a single latest turn summary with `status == "inProgress"` and a
   canonical non-nil UUID `id`. Any missing, completed, failed, or malformed turn
   summary fails closed (`WakeupOutcome::Failed`) before any effect write and
   never falls back to `turn/start` or `codex queue`.
4. **Exact active turn steer**: Sets `may_have_written = true` immediately before
   sending `turn/steer` with `expectedTurnId` set to the verified `inProgress`
   turn UUID and `input: [{"type": "text", "text": <fixed_wakeup_message>}]`.
   Requires the response to acknowledge the exact `expectedTurnId`. Post-write
   disconnects, timeouts, RPC errors, or mismatched turn IDs return
   `WakeupOutcome::Unknown` and are never retried automatically.

## Verification scope and limits

- Fake local Unix WebSocket tests cover serialization, digest separation, host
  summary transport reporting, idle `turn/start` routing, busy
  `thread/turns/list` + `turn/steer` routing, pre-effect refusal on missing or
  completed turns, and post-write `Unknown` classification on mismatched ACKs or
  RPC errors.
- Live desktop delivery in `user_message_steer` mode, app-closed behavior,
  multi-chat routing, and macOS/Windows support remain separate unverified gates.
