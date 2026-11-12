# Native Coworker Model Observation

## Configured vs. Observed Model Attribution

Threadlane separates native Codex chat configuration metadata from runtime-observed execution telemetry across both `ObservationActor` and `WorkspaceNativeSample`:

- `model` and `effort` (`ObservationActor`): Reserved exclusively for proven runtime-observed execution receipts. For a paired native Codex Coworker making MCP tool calls, these fields remain `None`.
- `selected_model` and `selected_effort` (`ObservationActor`): Optional (`#[serde(default, skip_serializing_if = "Option::is_none")]`) sanitized metadata tokens carrying the model and reasoning effort configured on the verified native Codex thread (`thread/read` with `includeTurns: false`).
- `WorkspaceConfiguredSelection` (`WorkspaceNativeSample`): Records `selected_model` and `selected_effort` with `evidence = verified_configuration` and `counts_as_runtime_telemetry = false`. Configured chat selection is never treated as observed runtime model execution telemetry.

## Verification and Refresh Invariants

1. **Verified Exact-Thread Provenance**:
   - `AutomaticPairing::connect_with_context_config` carries `model` and `effort` into `VerifiedMcpObservationContext` only after `verify_thread_hint_candidate` corroborates the thread ID, canonical `cwd`, `project_id`, and `connection_identity` on the existing managed Linux desktop parent app-server.
   - Workflow profile assignments and caller-supplied MCP arguments are never used to populate `ObservationActor`.
   - `native_per_call_identity_verified` remains `false`.

2. **Bounded Per-Request Refresh**:
   - On new paired MCP requests, `PairedMcpServer` refreshes the cached `VerifiedMcpObservationContext` at most once per request through the existing verified managed parent WebSocket endpoint (`OBSERVATION_METADATA_REFRESH_TIMEOUT`, metadata-only `thread/read` with `includeTurns: false`).
   - Refreshed metadata is accepted only if the parent reconnect endpoint and PID match and the returned `NativeThread` matches the verified `thread.id`, loaded status (`idle` or `active`), canonical `cwd`, and `project_id`.
   - If the refresh fails, times out, or mismatches scope, `selected_model` and `selected_effort` are cleared (`None`) so stale selection is never reported as current, while normal MCP tool calls remain usable.

## Continuous Workspace Native Chat Sampling

1. **`connect_current_chat` Gate and Producer Deduplication**:
   - Background native chat sampling starts only after `connect_current_chat` completes independent verification of the managed Linux desktop parent endpoint, loaded thread candidate, canonical repository `cwd`, `project_id`, control-plane binding, and `PairingStore` enrollment.
   - Caller `_meta.threadId` hints, `connection_identity`, or manual operator enrollment alone never start a sampler.
   - Repeat `connect_current_chat` calls matching the active `(connection_nonce, epoch_id, server_pid, chat_identity, project_id, backend_process_instance)` reuse the single running sampler without spawning a duplicate task or rotating `producer_epoch`.
   - Shared-connection quarantine, pairing/binding mismatch, `restart_epoch`, dropping `PairedMcpServer`, parent endpoint disconnect or replacement, or parent process exit / PID reuse immediately invalidate cached selection and terminate the background producer.

2. **Metadata-Only Transport and Sampling Cadence**:
   - Polls `thread/read` with `includeTurns: false` over the verified managed parent Unix WebSocket (`managed_linux_desktop` route) with default `WorkspaceSamplingPolicy`: `2,000 ms` (`2 s`) interval (`WORKSPACE_SAMPLING_INTERVAL_MS`), `750 ms` per-RPC timeout (`WORKSPACE_METADATA_TIMEOUT_MS`), and `10,000 ms` (`10 s`) staleness threshold (`WORKSPACE_SAMPLE_STALE_THRESHOLD_MS`, `snapshot_resets_sample_age: false`).
   - Never reads turns or raw conversation transcripts, never calls `thread/resume`, never starts a second `codex app-server`, never installs a persistent OS service, and never mutates global configuration or environment state.

3. **Identity, Producer Ordering, and Truthful Degradation**:
   - Samples are keyed by `WorkspaceChatIdentity` (`source_id` derived via `threadlane-observation-source-v1\0` SHA-256 framing over canonical `(plans_dir, authority_dir, sessions_dir)` or `"default"` on minimal unscoped stores, plus `binding_id`, `thread_uuid`, and `canonical_project_scope`), keeping co-scoped chats in the same repository isolated.
   - `reconcile_workspace_native_sample` orders samples using `ObservationProcessInstance` (`pid`, kernel `boot_id`, Linux `/proc/<pid>/stat` `start_ticks`), CSPRNG `producer_epoch` (`pepc_<64hex>`), and monotonic `sequence`, rejecting stale/duplicate sequences and epoch-process mismatches.
   - Only backend-proven structured `status` and `activeFlags` (`idle`, `active` with `[]`, `waitingOnApproval`, `waitingOnUserInput`) map to `idle`, `working`, `waiting_for_approval`, or `waiting_for_input`; plain `"active"` without `activeFlags`, unrecognized flags, or conflicting waiting flags degrade to `unknown`.
   - Whenever availability is `stale` (including read-time evaluation past `10,000 ms`), `unavailable`, or `disconnected`, activity degrades to `unknown` and configured model/effort selection is cleared (`unavailable`).

4. **Bounded Private Sidecar Store and Read-Only API**:
   - `write_workspace_native_sample_sidecar` atomically writes `0600` JSON sidecars (`sample-<sha256>.json`) inside `<state_dir>/native-chat-samples/` (`0700`, disjoint from `canonical_project_scope`) under `.native-chat-samples.lock`, enforcing `<= 64 KiB` (`MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES`) per sidecar and `<= 128` (`MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE`) retained samples per `source_id`.
   - Before installing a new retained incoming sidecar at the per-source bound, required validated stale-sample removal and parent directory `fsync` complete first under `.native-chat-samples.lock`; if cleanup fails (`IoError` before directory mutation, `PersistenceUncertain` after partial deletion or unconfirmed directory `fsync`, or `SecurityViolation` on symlink/non-regular entries), the new sidecar is not installed, per-source sample count never grows past `128`, and unrelated files or transcripts are never deleted.
   - `read_workspace_native_sample_readonly` and `read_workspace_native_samples_readonly` inspect and evaluate sidecars at `reference_now_ms` without acquiring locks, creating directories or files, or mutating persisted timestamps.

## Activity Feed Presentation

- When `selected_model` / `selected_effort` are present on a requester actor, the Activity feed formats them as `<model> · <effort> (selected)` (or `<model> (selected)`), appending to the requester line and timeline requester details while keeping runtime-observed `actorModel` / `actorEffort` as `Not reported` and leaving local MCP / worker executor rows untouched.

## Verification Boundaries

- Source-level invariants and Linux disposable fake-backend Unix WebSocket fixtures (`crates/threadlane-native/tests/native_chat_observation.rs` and `crates/threadlane-native/tests/native_ws_rpc.rs`) are distinct from executed Cargo gate results, live installed native Codex desktop verification, and non-Linux platform acceptance (`/proc` process-instance discovery and Unix private mode checks are Linux/Unix-specific).
