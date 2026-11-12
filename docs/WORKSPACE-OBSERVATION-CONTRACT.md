# Workspace Observation Contract

This document defines the shared read-only **Workspace Observation Contract** implemented in Rust (`crates/threadlane-core/src/observation.rs`) and TypeScript (`apps/desktop/src/observation/types.ts`, `apps/desktop/src/workspace-contract.ts`, and `apps/desktop/src/app-contract.ts`).

## Architectural & Compatibility Boundaries

1. **Core Independence**: `threadlane-core` owns the canonical wire schema, validation, sanitization, bounding, and freshness/reconciliation logic with **zero dependencies on Tauri or provider-specific CLIs**.
2. **Read-Only Non-Authority**: Observation envelopes, native conversation samples, safe `Messages` projections, and UI preferences are strictly read-only inspection surfaces. None of them grant scope approval, dispatch authority, model selection permission, or proof of human reading.
3. **100% Legacy Backward Compatibility**:
   - `ObservationSnapshot` and `ObservationActor` retain their exact existing JSON wire shapes, field semantics, and Rust struct-literal compatibility (`schema_version = 1`).
   - `WorkspaceSnapshot` is a **separate** outer envelope (`schema_version = 1`) that embeds `observation: ObservationSnapshot` alongside canonical chat summaries, native samples, structured job histories, and the bounded `Messages` window.
   - Legacy payloads and sparse/partial `WorkspaceSnapshot` payloads upgrade cleanly via `WorkspaceSnapshot::from_legacy_observation` / `parse_workspace_snapshot_json` (Rust) and `upgradeLegacyObservationSnapshot` / `parseWorkspaceSnapshot` (TypeScript), degrading any missing or unrecognized new fields safely to `unknown` or `unavailable`.
   - Legacy `unknown` job states remain diagnostic uncertainty on `job_histories[*].job.state` and never synthesize an actionable chat `attention_count` (`0`), matching `completed` and `failed` (`0`), while explicit `needs_user` retains `attention_count`.

---

## `WorkspaceSnapshot` Envelope

`WorkspaceSnapshot` carries:

- `schema_version`: `1` (`WORKSPACE_SNAPSHOT_SCHEMA_VERSION`)
- `generated_at_ms`: envelope assembly timestamp (ms since Unix epoch); assembling or regenerating a snapshot **never** resets native sample timestamps or sample age
- `sampling_policy`: `WorkspaceSamplingPolicy` (`2,000 ms` interval, `750 ms` metadata timeout, `10,000 ms` stale threshold, `snapshot_resets_sample_age: false`)
- `observation`: preserved `ObservationSnapshot` (`<= 500` jobs, `<= 200` requests/page)
- `chats`: canonical `WorkspaceChatSummary[]` sorted deterministically by canonical UTF-8 chat key
- `native_samples`: reconciled `WorkspaceNativeSample[]` (`<= 128` per `source_id`, each sidecar `<= 64 KiB`)
- `job_histories`: structured `WorkspaceJobHistory[]` (`<= 64` per binding, `<= 500` total)
- `messages`: safe `WorkspaceMessagesWindow` (`<= 1,000` items; `requires_transcript: false`, `allows_synthetic_user_messages: false`)
- `bounds`: `WorkspaceSnapshotBounds` recording omitted/capped counts across all windows

---

## Authoritative Chat Identity (`WorkspaceChatIdentity`)

Every workspace chat is identified strictly by four validated fields:

1. `source_id`: non-empty trimmed ASCII identifier (`[A-Za-z0-9_.-]+`, `<= 128` UTF-8 bytes)
2. `binding_id`: non-empty trimmed ASCII correlation identifier (`[A-Za-z0-9_.:-]+`, `<= 128` UTF-8 bytes)
3. `thread_uuid`: canonical lowercase hyphenated non-nil UUID (`36` ASCII chars)
4. `canonical_project_scope`: normalized trimmed absolute UTF-8 path (`<= 512` UTF-8 bytes; no control characters, `//`, `.` or `..` segments, or trailing `/`)

### Non-Substitutability Rules

- **Display chat titles (`thread_label`)** are mutable display metadata only; renaming a thread never alters chat identity.
- **Provider/shared-fork `sessionId` tokens** (`sess_*`, `session_*`, `sessionId*`) are never accepted as `source_id`, `binding_id`, or `thread_uuid`.
- **Execution `job_id` and `plan_id` tokens** (`job_*`, `plan_*`) identify work units inside or across chats and never substitute for chat identity.

### Collision-Free Canonical Key

Both Rust (`WorkspaceChatIdentity::canonical_key`) and TypeScript (`formatWorkspaceChatIdentityKey`) compute an identical UTF-8 byte-length-prefixed key:

```text
wchat:v1:<b1>:<source_id>:<b2>:<binding_id>:<b3>:<thread_uuid>:<b4>:<canonical_project_scope>
```

where `<b1>..<b4>` are exact UTF-8 byte lengths (not UTF-16 code units or Unicode scalar counts).

---

## Native Conversation Samples, Availability vs. Activity, and Configured Selection

### `WorkspaceNativeSample`

Each native sample carries:

- `chat_identity`: validated `WorkspaceChatIdentity`
- `producer_epoch`: non-empty trimmed ASCII token identifying a single producer lifecycle
- `backend_process_instance`: optional `ObservationProcessInstance { pid, boot_id, start_ticks }` corroborating OS process identity across PID reuse
- `sequence`: monotonic `u64` / non-negative integer sequence within `producer_epoch`
- `observed_at_ms`: producer observation timestamp (`last_success_at_ms <= observed_at_ms` when both are present)
- `last_success_at_ms`: most recent successful verified metadata read timestamp (sole clock anchor for sample age)
- `availability`: `available | disconnected | stale | unavailable`
- `activity`: `working | idle | waiting_for_approval | waiting_for_input | unknown`
- `connection`: `{ state: 'connected' | 'disconnected' | 'degraded' | 'unavailable', freshness: 'fresh' | 'stale' | 'never_succeeded' | 'unknown', route, diagnostic }`
- `capabilities`: tri-state (`supported | unsupported | unknown`) flags for `exact_thread_read`, `activity_detail`, `configured_selection`, `wakeup_delivery`, and `per_call_identity`
- `configured_selection`: `WorkspaceConfiguredSelection`

### Strict Separation of Availability and Live Activity

- **Availability** (`available`, `disconnected`, `stale`, `unavailable`) describes whether the observation route is currently reachable and fresh.
- **Activity** (`working`, `idle`, `waiting_for_approval`, `waiting_for_input`, `unknown`) describes live thread execution state **only** when effective availability is `available`.
- Whenever effective availability is `disconnected`, `stale`, or `unavailable`, effective `activity` is forced to `unknown` so stale or disconnected threads are never presented as actively `working` or `idle`.

### `selected_model` & `selected_effort` as Verified Configuration Evidence

- `WorkspaceConfiguredSelection` carries `selected_model`, `selected_effort`, `evidence: 'verified_configuration' | 'unavailable'`, and `counts_as_runtime_telemetry: false`.
- `counts_as_runtime_telemetry` is structurally pinned to `false` on normalization in both Rust and TypeScript.
- Missing, unverified, disconnected, or stale samples clear `selected_model` and `selected_effort` to `null` with `evidence: 'unavailable'`.
- Configured Coworker selection never populates runtime execution fields (`ObservationActor.model` / `ObservationActor.effort`). See [Native Coworker Model Observation](COWORKER-MODEL-OBSERVATION.md).

### Producer Epoch & Monotonic Sequence Reconciliation

`reconcile_workspace_native_sample` / `reconcileWorkspaceNativeSample` enforces deterministic ordering per `WorkspaceChatIdentity`:

1. Invalid incoming samples or mismatched `chat_identity` values are rejected (`rejected_invalid_sample`, `rejected_mismatched_identity`).
2. Within the **same `producer_epoch`**:
   - Mismatched `backend_process_instance` (`pid`, `boot_id`, or `start_ticks`) is rejected (`rejected_epoch_process_mismatch`).
   - Strictly greater `sequence` is accepted (`accepted_next_sequence`).
   - Equal or lower `sequence` is rejected (`rejected_duplicate_sequence`, `rejected_stale_sequence`).
3. Across a **different `producer_epoch`** (producer restart):
   - Accepted (`accepted_new_epoch`) only when `incoming.observed_at_ms` is present and `>= current.observed_at_ms`; older restart samples are rejected (`rejected_stale_epoch`).

### Initial Sampling & Staleness Policy (`WorkspaceSamplingPolicy`)

| Parameter | Value | Meaning |
| --- | --- | --- |
| `interval_ms` | `2,000 ms` (`2 s`) | Initial native conversation sampling interval |
| `metadata_timeout_ms` | `750 ms` | Bounded metadata read timeout (matches existing `OBSERVATION_METADATA_REFRESH_TIMEOUT`) |
| `stale_threshold_ms` | `10,000 ms` (`10 s`) | Maximum sample age (`reference_now_ms - last_success_at_ms <= 10,000 ms`) before `available` degrades to `stale` |
| `snapshot_resets_sample_age` | `false` | Assembling or regenerating `WorkspaceSnapshot` never mutates `observed_at_ms` or `last_success_at_ms` |

---

## Structured Job History (`WorkspaceJobHistory`)

`WorkspaceJobHistory` keeps lifecycle, execution, audit links, failure history, live attention, and receipt-backed acceptance in distinct typed fields:

- `job`: `WorkspaceJobSummary` (`source_id`, `job_id`, `binding_id`, optional `chat_identity`, `title`, `job_revision`, `state`, `phase`, `approval_status`, `execution_status`, `review_state`, timestamps, `summary`)
- `tasks`: `WorkspaceTaskHistory[]` (`task_id`, `title`, `role`, `difficulty`, `stage`, `status`, `depends_on`, `current_attempt_no`, `attempts`, `candidates`, `active_candidate_id`, `accepted_candidate_id`, task `historical_failure`, task `current_attention`, task `acceptance_proof`)
- `runs`: `WorkspaceRunRecord[]` (`run_id`, `worker_id`, `binding_id`, `task_id`, `attempt_no`, `role`, `status`, `executor`, timestamps)
- `outbox`: `WorkspaceOutboxRecord[]` (`outbox_id`, `request_key`, `kind`, `status`, `task_id`, `attempt_no`, `role`, `run_id`, `worker_id`, `receipt_id`, job revisions, `diagnostic`)
- `combined_candidates`: `WorkspaceCombinedCandidate[]` (`combined_candidate_id`, `job_revision`, `status`, `final_commit_sha`, `final_tree_sha`, `candidate_digest`, `closure_reservation_id`, `check_count`, `produced_at_ms`)
- `result`: optional `WorkspaceJobResult` (`outcome`, `summary`, `produced_at_ms`, `delivery_stage`, `closure_verdict_id`) — transport submission or batch ACK never proves human reading
- `links`: `WorkspaceRecordLinks` (`source_plan_id`, `source_plan_revision`, `source_approval_id`, `source_work_brief_id`, `planning_grant_id`, `planning_reservation_id`, `supersedes_job_id`, `superseded_by_job_id`, `parent_job_id`, `follow_up_job_ids`, `architect_advice_ids`, `source_request_ids`)
- `historical_failure`: optional `WorkspaceHistoricalFailure` (`failure_id`, `stage`, `failed_at_ms`, `task_id`, `attempt_no`, `run_id`, `outbox_id`, `reason`, `superseded_or_resolved`) — preserved for audit even after a later retry succeeds
- `current_attention`: optional `WorkspaceAttentionState` (`kind`, `reason`, `next_step`, `blocking_hold_ids`, `blocking_outbox_ids`, `question_id`, `updated_at_ms`) — live operator blocker/action state kept distinct from historical failures
- `acceptance_proof`: `WorkspaceAcceptanceProof`:
  - **Task `verified_accepted`** requires a valid `accepted_candidate_id` **and** at least one of `review_verdict_id` or `review_receipt_id`; otherwise `verified_accepted` degrades to `unknown`.
  - **Job `verified_accepted`** requires both a valid `combined_candidate_id` **and** an accepted `closure_verdict_id`; otherwise `verified_accepted` degrades to `unknown`.
  - Worker completion or verification checks alone never satisfy `verified_accepted`.
- `external_resolutions`: optional `WorkspaceExternalResolution[]` projected only from real private-ledger `external_resolution_receipts_v1` records (`receipt_id`, `request_key`, `preview_digest`, `outcome: 'resolved_externally'`, `scope: 'job' | 'tasks'`, `task_ids`, `applicability: 'applicable' | 'stale' | 'unverified'`, `summary`, `evidence_refs`, `recorded_at_ms`, `covered_attention`). The field defaults to an empty list and is omitted when empty for backward-compatible sparse/legacy payloads.
- `truncation`: `WorkspaceJobHistoryTruncation` counting omitted tasks, attempts, task candidates, runs, outbox entries, combined candidates, and invalid/duplicate/overflow external resolution receipts in optional `external_resolutions_omitted` (omitted from JSON when zero).

External resolution applicability is re-evaluated on every store-backed read from current job revision, controller runtime, plan, approval, conversational-change, job-ownership, and host evidence. It is a logical attestation, not a process-local durability/fsync witness. `applicable` full-job coverage may retire only the exact current coverable attention rows and task-scoped coverage only its declared tasks; `stale` and `unverified` history remains visible and never hides `unknown`, unresolved outbox/effect, unrelated task, or new native human-question blockers. A fully covered job may project the separate logical `execution_status: 'resolved_externally'` with `state: 'unknown'`, but never `completed` or `verified_accepted`, and stored phases, worker outcomes, real results, holds, envelopes, review/integration/Closure proof, and ACK state remain unchanged. Older readers that do not know the field simply see the prior history shape; no bidirectional mixed-version compatibility beyond that omission is claimed.

---

## Safe `Messages` Projection (`WorkspaceMessagesWindow`)

`Messages` provides a chat-scoped readable timeline derived **exclusively** from existing safe summary, result, and question records:

1. `work_brief_summary`
2. `plan_summary`
3. `question_prompt`
4. `question_answer`
5. `task_result_summary`
6. `review_verdict_summary`
7. `closure_summary`
8. `job_result_summary`
9. `attention_summary`
10. `journal_summary`

### Safety & Completeness Invariants

- **No Transcripts or Synthetic Turns**: Raw provider transcripts (`transcript`, `raw_transcript`, `provider_transcript`) and synthetic user turns (`synthetic_user_message`, `user_chat_turn`) are rejected (`ForbiddenMessageSource` / `'forbidden_message_source'`). `requires_transcript` and `allows_synthetic_user_messages` are pinned to `false`.
- **Deterministic Source Identity**: Each `WorkspaceMessageItem` computes `wmsg:v1:<b1>:<source_id>:<b2>:<source_kind>:<b3>:<source_record_id>:<sub_index>`.
- **Deterministic Ordering**: Items sort by `(recorded_at_ms.is_some(), recorded_at_ms, source_seq, sub_index, source_id, source_kind, source_record_id, item_id)` and retain the newest `<= 1,000` items.
- **Explicit Scope & Completeness**: `WorkspaceMessageScope` carries `source_id`, optional `binding_id`, `thread_uuid`, `canonical_project_scope`, validated `chat_identity` (when all four components are present), `job_id`, `task_id`, `run_id`, and `attempt_no`. `WorkspaceMessageCompleteness` records `summary_truncated`, `original_char_count`, `max_summary_chars` (`240`), `redacted`, `missing_fields`, and `incomplete_reason`.

---

## Shared Frontend Surface, Per-Chat Archive, and Compact Tray Contracts

Shared TypeScript definitions in `apps/desktop/src/workspace-contract.ts` and `apps/desktop/src/app-contract.ts` pin:

1. **Chat-First Tabs (`WORKSPACE_CHAT_SURFACE_TABS`)**:
   - `['messages', 'jobs', 'activity']` with `DEFAULT_WORKSPACE_CHAT_TAB = 'messages'`.
   - `WORKSPACE_SURFACE_SEPARATION` explicitly separates:
     - `Messages`: safe summaries, results, and questions (`<= 1,000` items, no transcripts or synthetic user turns).
     - `Jobs`: durable job/task/attempt/candidate/closure history ledger.
     - `Activity`: paginated low-level MCP/HTTP/CLI request and delivery audit feed (`50` default page, `200` max page, `1,000` frontend request window).
2. **Per-Chat `Active` / `Archive` Preferences**:
   - Lifecycle values `['active', 'archived']` (`DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER = 'active'`).
   - Persisted under `WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY` (`threadlane.desktop.workspace.chatArchive.v1`), keyed **strictly** by canonical `wchat:v1:...` chat identity (`<= 512` archived entries). Entries keyed by chat title, `sessionId`, or `job_id` are rejected on write and ignored on read.
3. **Compact Native Tray Bounds (`WORKSPACE_TRAY_CONTENT_BOUNDS`)**:
   - At most `3` compact rows (`WORKSPACE_TRAY_MAX_ROWS = 3`) prioritized strictly as `'needs_human' -> 'working' -> 'result'`.
   - At most `1` terminal result row (`WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS = 1`).
   - Snapshot polling bound of `50` requests (`WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT = 50`) at `4,000 ms` refresh cadence (`WORKSPACE_TRAY_REFRESH_INTERVAL_MS = 4_000`).
   - Unicode-safe compact text caps: `80` title chars, `160` detail chars, `200` error chars.
   - Explicit window actions `['show_main', 'hide', 'quit']` and close behaviors `['hide_to_tray', 'quit']`.

---

## Pinned Numeric Bounds

Existing observation bounds are preserved without increase, and new sidecar, `Messages`, and job-history projection bounds stay within existing ledger capacities:

| Bound Constant | Value | Scope |
| --- | --- | --- |
| `DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT` | `50` | Default paginated `ObservationSnapshot.requests` page size (preserved) |
| `MAX_OBSERVATION_REQUEST_PAGE_LIMIT` | `200` | Maximum `ObservationSnapshot.requests` records per page (preserved) |
| `MAX_OBSERVATION_JOBS` | `500` | Maximum jobs per `ObservationSnapshot` / `WorkspaceSnapshot` (preserved) |
| `MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW` | `1,000` | Maximum accumulated requests in frontend `Activity` window (preserved) |
| `MAX_OBSERVATION_SUMMARY_CHARS` | `240` | Maximum Unicode scalar values per sanitized safe summary (preserved) |
| `MAX_WORKSPACE_LABEL_BYTES` | `128` | Maximum UTF-8 bytes per sanitized identifier/label (preserved) |
| `MAX_WORKSPACE_CANONICAL_PATH_BYTES` | `512` | Maximum UTF-8 bytes per `canonical_project_scope` path (preserved) |
| `MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES` | `65,536` (`64 KiB`) | Maximum serialized UTF-8 byte length of a single native sample sidecar |
| `MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE` | `128` | Maximum retained native sample files/records per `source_id` |
| `MAX_WORKSPACE_MESSAGES_WINDOW` | `1,000` | Maximum retained `WorkspaceMessageItem` entries in `WorkspaceMessagesWindow` |
| `MAX_WORKSPACE_JOBS_PER_BINDING` | `64` | Maximum projected `WorkspaceJobHistory` entries per `(source_id, binding_id)` |
| `MAX_WORKSPACE_TASKS_PER_JOB` | `64` | Maximum projected `WorkspaceTaskHistory` entries per job |
| `MAX_WORKSPACE_ATTEMPTS_PER_TASK` | `16` | Maximum projected `WorkspaceTaskAttempt` entries per task |
| `MAX_WORKSPACE_CANDIDATES_PER_TASK` | `16` | Maximum projected `WorkspaceTaskCandidate` entries per task |
| `MAX_WORKSPACE_RUNS_PER_JOB` | `256` | Maximum projected `WorkspaceRunRecord` entries per job |
| `MAX_WORKSPACE_OUTBOX_PER_JOB` | `256` | Maximum projected `WorkspaceOutboxRecord` entries per job |
| `MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB` | `32` | Maximum projected `WorkspaceCombinedCandidate` entries per job |
| `MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB` | `64` | Maximum projected `WorkspaceExternalResolution` receipts per job (invalid/duplicate/overflow rows counted in `external_resolutions_omitted`) |

---

## Verification Coverage

Behavioral parity and failure/degradation invariants are verified across both languages in:

- `crates/threadlane-core/tests/workspace_observation_contract.rs`
- `apps/desktop/src/workspace-contract.test.ts`

Both test suites share an identical golden `WorkspaceSnapshot` JSON fixture and test legacy `ObservationSnapshot` / `ObservationActor` compatibility, sparse/future enum degradation to `unknown` / `unavailable`, four-part `WorkspaceChatIdentity` validation, `producer_epoch` + `ObservationProcessInstance` + monotonic `sequence` reconciliation, availability vs. activity separation, `selected_model` / `selected_effort` configuration evidence labeling, `2 s / 750 ms / 10 s` sampling and snapshot non-reset of sample age, receipt-backed task/job `acceptance_proof`, store-verified external resolution history and omission accounting, safe `Messages` source filtering, multi-byte UTF-8 / emoji boundaries, frontend surface/archive/tray contracts, and all numeric bounds. Native history and Tauri fixture suites additionally exercise real `ExternalResolutionService` record receipts for full/partial coverage, mixed blockers, stale/unverified/forged/tampered evidence, and read-only no-write snapshots after recording.
