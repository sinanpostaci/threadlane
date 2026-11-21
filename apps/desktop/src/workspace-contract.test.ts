import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER,
  DEFAULT_WORKSPACE_CHAT_TAB,
  OPEN_CODEX_CHAT_COMMAND,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_CHAT_LIFECYCLE_FILTERS,
  WORKSPACE_CHAT_SURFACE_TABS,
  WORKSPACE_MAIN_CLOSE_BEHAVIORS,
  WORKSPACE_SURFACE_SEPARATION,
  WORKSPACE_TRAY_ACTIONS,
  WORKSPACE_TRAY_CONTENT_BOUNDS,
  WORKSPACE_TRAY_MAX_DETAIL_CHARS,
  WORKSPACE_TRAY_MAX_ERROR_CHARS,
  WORKSPACE_TRAY_MAX_ROWS,
  WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS,
  WORKSPACE_TRAY_MAX_TITLE_CHARS,
  WORKSPACE_TRAY_REFRESH_INTERVAL_MS,
  WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT,
  boundWorkspaceCompactTrayRows,
  normalizeWorkspaceChatTab,
  readWorkspaceChatArchivePreferences,
  resolveWorkspaceChatLifecycle,
  setWorkspaceChatArchivePreference,
  writeWorkspaceChatArchivePreferences,
} from './app-contract.ts';
import type {
  CodexChatTarget,
  WorkspaceChatPreferenceStorage,
  WorkspaceCompactTrayRow,
  WorkspaceScopedChatTarget,
} from './app-contract.ts';
import type { ObservationActor, ObservationSnapshot } from './observation/types.ts';
import {
  DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
  DEFAULT_WORKSPACE_SAMPLING_POLICY,
  MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW,
  MAX_OBSERVATION_JOBS,
  MAX_OBSERVATION_REQUEST_PAGE_LIMIT,
  MAX_OBSERVATION_SUMMARY_CHARS,
  MAX_WORKSPACE_ATTEMPTS_PER_TASK,
  MAX_WORKSPACE_CANDIDATES_PER_TASK,
  MAX_WORKSPACE_CHAT_WORK_ITEMS_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_TOTAL,
  MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT,
  MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB,
  MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION,
  MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB,
  MAX_WORKSPACE_JOBS_PER_BINDING,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE,
  MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
  MAX_WORKSPACE_OUTBOX_PER_JOB,
  MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
  MAX_WORKSPACE_RUNS_PER_JOB,
  MAX_WORKSPACE_TASKS_PER_JOB,
  WORKSPACE_EXTERNAL_RESOLUTION_APPLICABILITIES,
  WORKSPACE_EXTERNAL_RESOLUTION_OUTCOMES,
  WORKSPACE_EXTERNAL_RESOLUTION_SCOPES,
  WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT,
  WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION,
  WORKSPACE_METADATA_TIMEOUT_MS,
  WORKSPACE_SAFE_MESSAGE_SOURCE_KINDS,
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  WORKSPACE_SAMPLING_INTERVAL_MS,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
  WorkspaceContractError,
  boundWorkspaceJobHistories,
  boundWorkspaceNativeSamplesPerSource,
  createSafeWorkspaceMessageItem,
  createVerifiedConfiguredSelection,
  evaluateWorkspaceNativeSampleAt,
  formatWorkspaceChatIdentityKey,
  normalizeWorkspaceChatSummary,
  normalizeWorkspaceChatWork,
  normalizeWorkspaceExternalResolution,
  normalizeWorkspaceExternalResolutions,
  normalizeWorkspaceJobHistory,
  normalizeWorkspaceNativeSample,
  normalizeWorkspaceSnapshot,
  parseWorkspaceNativeSampleSidecar,
  parseWorkspaceSnapshot,
  projectSafeWorkspaceMessages,
  reconcileWorkspaceNativeSample,
  regenerateWorkspaceSnapshotAt,
  sanitizeWorkspaceSummaryWithMeta,
  serializeWorkspaceNativeSampleSidecar,
  upgradeLegacyObservationSnapshot,
  utf8ByteLength,
  validateSafeWorkspaceMessageSource,
  validateWorkspaceChatIdentity,
  workspaceChatIdentitiesEqual,
} from './workspace-contract.ts';
import type {
  ObservationProcessInstance,
  WorkspaceChatIdentity,
  WorkspaceExternalResolution,
  WorkspaceJobHistory,
  WorkspaceNativeSample,
} from './workspace-contract.ts';

const VALID_UUID_A = '0196ed00-0000-7000-8000-000000000042';
const VALID_UUID_B = '0196ed00-0000-7000-8000-000000000099';

function sampleIdentity(): WorkspaceChatIdentity {
  return validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_chat_alpha',
    thread_uuid: VALID_UUID_A,
    canonical_project_scope: '/home/dev/repos/alpha-app',
  });
}

function sampleProcessInstance(pid: number, startTicks: number): ObservationProcessInstance {
  return {
    pid,
    boot_id: 'boot-linux-01',
    start_ticks: startTicks,
  };
}

function sampleNativeLive(
  identity: WorkspaceChatIdentity,
  epoch: string,
  processInstance: ObservationProcessInstance | null,
  sequence: number,
  observedAtMs: number,
): WorkspaceNativeSample {
  return {
    chat_identity: identity,
    producer_epoch: epoch,
    backend_process_instance: processInstance,
    sequence,
    observed_at_ms: observedAtMs,
    last_success_at_ms: observedAtMs,
    availability: 'available',
    activity: 'working',
    connection: {
      state: 'connected',
      freshness: 'fresh',
      route: 'managed_linux_desktop',
      diagnostic: null,
    },
    capabilities: {
      exact_thread_read: 'supported',
      activity_detail: 'supported',
      configured_selection: 'supported',
      wakeup_delivery: 'supported',
      per_call_identity: 'supported',
    },
    configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
  };
}

test('golden WorkspaceSnapshot JSON fixture matches Rust wire and semantic contract 1:1', () => {
  // Shared golden JSON fixture mirrored verbatim in
  // `crates/threadlane-core/tests/workspace_observation_contract.rs`.
  const goldenJson = `{
        "schema_version": 1,
        "generated_at_ms": 1700000010000,
        "sampling_policy": {
            "interval_ms": 2000,
            "metadata_timeout_ms": 750,
            "stale_threshold_ms": 10000,
            "snapshot_resets_sample_age": false
        },
        "observation": {
            "schema_version": 1,
            "generated_at_ms": 1700000010000,
            "sources": [
                {
                    "id": "default",
                    "label": "alpha-app",
                    "project_path": "/home/dev/repos/alpha-app",
                    "availability": "available",
                    "message": null
                }
            ],
            "jobs": [],
            "requests": [],
            "next_cursor": null,
            "has_more": false,
            "jobs_has_more": false,
            "warnings": []
        },
        "chats": [
            {
                "identity": {
                    "source_id": "default",
                    "binding_id": "bnd_chat_alpha",
                    "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                    "canonical_project_scope": "/home/dev/repos/alpha-app"
                },
                "thread_label": "Alpha Chat",
                "project_label": "alpha-app",
                "availability": "available",
                "activity": "waiting_for_input",
                "configured_selection": {
                    "selected_model": "gpt-5.4",
                    "selected_effort": "high",
                    "evidence": "verified_configuration",
                    "counts_as_runtime_telemetry": false
                },
                "active_job_count": 1,
                "attention_count": 1,
                "updated_at_ms": 1700000008000
            }
        ],
        "native_samples": [
            {
                "chat_identity": {
                    "source_id": "default",
                    "binding_id": "bnd_chat_alpha",
                    "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                    "canonical_project_scope": "/home/dev/repos/alpha-app"
                },
                "producer_epoch": "epoch_golden_01",
                "backend_process_instance": {
                    "pid": 4242,
                    "boot_id": "boot-linux-01",
                    "start_ticks": 900001
                },
                "sequence": 7,
                "observed_at_ms": 1700000008000,
                "last_success_at_ms": 1700000008000,
                "availability": "available",
                "activity": "waiting_for_input",
                "connection": {
                    "state": "connected",
                    "freshness": "fresh",
                    "route": "managed_linux_desktop",
                    "diagnostic": null
                },
                "capabilities": {
                    "exact_thread_read": "supported",
                    "activity_detail": "supported",
                    "configured_selection": "supported",
                    "wakeup_delivery": "supported",
                    "per_call_identity": "supported"
                },
                "configured_selection": {
                    "selected_model": "gpt-5.4",
                    "selected_effort": "high",
                    "evidence": "verified_configuration",
                    "counts_as_runtime_telemetry": false
                }
            }
        ],
        "job_histories": [
            {
                "job": {
                    "source_id": "default",
                    "job_id": "job_golden_01",
                    "binding_id": "bnd_chat_alpha",
                    "chat_identity": {
                        "source_id": "default",
                        "binding_id": "bnd_chat_alpha",
                        "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                        "canonical_project_scope": "/home/dev/repos/alpha-app"
                    },
                    "title": "Golden Contract Package",
                    "job_revision": 4,
                    "state": "completed",
                    "phase": "completed",
                    "approval_status": "approved",
                    "execution_status": "accepted",
                    "review_state": "review_passed",
                    "created_at_ms": 1700000001000,
                    "updated_at_ms": 1700000007500,
                    "summary": "Verified shared contract."
                },
                "tasks": [
                    {
                        "task_id": "task_golden_01",
                        "title": "Define WorkspaceSnapshot",
                        "role": "coder",
                        "difficulty": "hard",
                        "stage": "accepted",
                        "status": "accepted",
                        "depends_on": [],
                        "current_attempt_no": 1,
                        "attempts": [
                            {
                                "attempt_no": 1,
                                "status": "completed",
                                "run_id": "run_golden_01",
                                "worker_id": "w_golden_01",
                                "outbox_id": "outbox_golden_01",
                                "executor": null,
                                "started_at_ms": 1700000002000,
                                "finished_at_ms": 1700000005000,
                                "summary": "Implemented contract types."
                            }
                        ],
                        "candidates": [
                            {
                                "candidate_id": "cand_golden_01",
                                "attempt_no": 1,
                                "worker_id": "w_golden_01",
                                "head_commit_sha": "111122223333",
                                "tree_sha": "444455556666",
                                "verification_passed": true,
                                "review_status": "accepted",
                                "review_verdict_id": "rverd_golden_01",
                                "review_receipt_id": "rrcpt_golden_01",
                                "produced_at_ms": 1700000005000,
                                "summary": "Candidate passed checks."
                            }
                        ],
                        "active_candidate_id": null,
                        "accepted_candidate_id": "cand_golden_01",
                        "historical_failure": null,
                        "current_attention": null,
                        "acceptance_proof": {
                            "status": "verified_accepted",
                            "accepted_candidate_id": "cand_golden_01",
                            "combined_candidate_id": null,
                            "review_verdict_id": "rverd_golden_01",
                            "review_receipt_id": "rrcpt_golden_01",
                            "integration_receipt_id": null,
                            "closure_reservation_id": null,
                            "closure_verdict_id": null,
                            "verified_at_ms": 1700000006000
                        }
                    }
                ],
                "runs": [],
                "outbox": [],
                "combined_candidates": [
                    {
                        "combined_candidate_id": "ccand_golden_01",
                        "job_revision": 4,
                        "status": "accepted",
                        "final_commit_sha": "777788889999",
                        "final_tree_sha": "aaaabbbbcccc",
                        "candidate_digest": "digest_golden_01",
                        "closure_reservation_id": "clos_res_golden_01",
                        "check_count": 3,
                        "produced_at_ms": 1700000006500
                    }
                ],
                "result": {
                    "outcome": "completed",
                    "summary": "Workspace contract accepted by Closure.",
                    "produced_at_ms": 1700000007500,
                    "delivery_stage": "result_ready",
                    "closure_verdict_id": "cverd_golden_01"
                },
                "links": {
                    "source_plan_id": "plan_golden_01",
                    "source_plan_revision": 1,
                    "source_approval_id": "apr_golden_01",
                    "source_work_brief_id": "brief_golden_01",
                    "planning_grant_id": null,
                    "planning_reservation_id": null,
                    "supersedes_job_id": null,
                    "superseded_by_job_id": null,
                    "parent_job_id": null,
                    "follow_up_job_ids": [],
                    "architect_advice_ids": [],
                    "source_request_ids": []
                },
                "historical_failure": null,
                "current_attention": null,
                "acceptance_proof": {
                    "status": "verified_accepted",
                    "accepted_candidate_id": null,
                    "combined_candidate_id": "ccand_golden_01",
                    "review_verdict_id": null,
                    "review_receipt_id": null,
                    "integration_receipt_id": "ircpt_golden_01",
                    "closure_reservation_id": "clos_res_golden_01",
                    "closure_verdict_id": "cverd_golden_01",
                    "verified_at_ms": 1700000007500
                },
                "truncation": {
                    "tasks_omitted": 0,
                    "attempts_omitted": 0,
                    "candidates_omitted": 0,
                    "runs_omitted": 0,
                    "outbox_omitted": 0,
                    "combined_candidates_omitted": 0
                }
            }
        ],
        "messages": {
            "availability": "available",
            "items": [
                {
                    "item_id": "wmsg:v1:7:default:15:closure_summary:15:cverd_golden_01:0",
                    "source_kind": "closure_summary",
                    "source_record_id": "cverd_golden_01",
                    "source_order": {
                        "recorded_at_ms": 1700000007500,
                        "source_seq": 4,
                        "sub_index": 0
                    },
                    "scope": {
                        "source_id": "default",
                        "binding_id": "bnd_chat_alpha",
                        "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                        "canonical_project_scope": "/home/dev/repos/alpha-app",
                        "chat_identity": {
                            "source_id": "default",
                            "binding_id": "bnd_chat_alpha",
                            "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
                            "canonical_project_scope": "/home/dev/repos/alpha-app"
                        },
                        "job_id": "job_golden_01",
                        "task_id": null,
                        "run_id": null,
                        "attempt_no": null
                    },
                    "actor_role": "closure",
                    "summary": "Workspace contract accepted by Closure.",
                    "completeness": {
                        "summary_truncated": false,
                        "original_char_count": 39,
                        "max_summary_chars": 240,
                        "redacted": false,
                        "missing_fields": [],
                        "incomplete_reason": null
                    }
                }
            ],
            "max_items": 1000,
            "omitted_count": 0,
            "has_more": false,
            "truncated": false,
            "requires_transcript": false,
            "allows_synthetic_user_messages": false
        },
        "bounds": {
            "native_samples_omitted": 0,
            "job_histories_omitted": 0,
            "messages_omitted": 0,
            "requests_capped": false,
            "jobs_capped": false
        }
    }`;

  const snap = parseWorkspaceSnapshot(goldenJson);
  assert.equal(snap.schema_version, WORKSPACE_SNAPSHOT_SCHEMA_VERSION);
  assert.equal(snap.chats.length, 1);

  const expectedGoldenKey =
    'wchat:v1:7:default:14:bnd_chat_alpha:36:0196ed00-0000-7000-8000-000000000042:25:/home/dev/repos/alpha-app';
  assert.equal(formatWorkspaceChatIdentityKey(snap.chats[0]!.identity), expectedGoldenKey);
  assert.equal(snap.chats[0]!.availability, 'available');
  assert.equal(snap.chats[0]!.activity, 'waiting_for_input');
  assert.equal(snap.chats[0]!.configured_selection.selected_model, 'gpt-5.4');
  assert.equal(snap.chats[0]!.configured_selection.evidence, 'verified_configuration');
  assert.equal(snap.chats[0]!.configured_selection.counts_as_runtime_telemetry, false);

  const evaluated = evaluateWorkspaceNativeSampleAt(
    snap.native_samples[0]!,
    snap.generated_at_ms,
    10_000,
  );
  assert.equal(evaluated.sample_age_ms, 2_000);
  assert.equal(evaluated.freshness, 'fresh');
  assert.equal(evaluated.effective_availability, 'available');
  assert.equal(evaluated.effective_activity, 'waiting_for_input');

  assert.equal(snap.job_histories[0]!.acceptance_proof.status, 'verified_accepted');
  assert.equal(snap.job_histories[0]!.tasks[0]!.acceptance_proof.status, 'verified_accepted');
  assert.equal(
    snap.messages.items[0]!.item_id,
    'wmsg:v1:7:default:15:closure_summary:15:cverd_golden_01:0',
  );

  const reserialized = JSON.stringify(snap);
  const reparsed = parseWorkspaceSnapshot(reserialized);
  assert.deepEqual(reparsed, snap);
});

test('chat_work defaults empty, rejects malformed scope/status, dedupes by source event id, and preserves actual timestamps', () => {
  const legacy = normalizeWorkspaceSnapshot({});
  assert.deepEqual(legacy.chat_work, []);

  const identity = sampleIdentity();
  const itemId = 'blg_chat_work_item';
  const doneItem = {
    item_id: itemId,
    binding_id: identity.binding_id,
    revision: 5,
    title: 'Teslim edilen iş',
    acceptance_condition: 'The delivered result is checked.',
    delivery_description: 'Deliver the checked result.',
    kind: 'worker',
    status: 'done',
    blocked_reason: null,
    current_execution_ref: 'wbe_chat_work_execution',
    completion_note: 'Checked and accepted',
    completion_evidence: 'Inspected the delivered artifact',
    completed_at_ms: 1_700_000_000_900,
    created_at_ms: 1_700_000_000_100,
    updated_at_ms: 1_700_000_000_900,
  };
  const failedEvent = {
    event_id: `${itemId}:4`,
    sequence: 2,
    item_id: itemId,
    revision: 4,
    action: 'worker_outcome_recorded',
    recorded_at_ms: 1_700_000_000_800,
    title: 'Kontrol edilen sonuç',
    status: 'doing',
    execution_ref: 'wbe_chat_work_execution',
    outcome: 'success',
    note: 'Verified technical success',
  };
  const doingItemId = 'blg_chat_work_running';
  const doingItem = {
    ...doneItem,
    item_id: doingItemId,
    revision: 2,
    status: 'doing',
    current_execution_ref: 'wbe_chat_work_running',
    completion_note: null,
    completion_evidence: null,
    completed_at_ms: null,
    created_at_ms: 1_700_000_000_200,
    updated_at_ms: 1_700_000_000_850,
  };
  const completedEvent = {
    event_id: `${itemId}:5`,
    sequence: 10,
    item_id: itemId,
    revision: 5,
    action: 'completed',
    recorded_at_ms: 1_700_000_000_900,
    title: 'Teslim edilen iş',
    status: 'done',
    execution_ref: 'wbe_chat_work_execution',
    outcome: null,
    note: 'Checked and accepted',
  };
  const presence = {
    item_id: doingItemId,
    execution_ref: 'wbe_chat_work_running',
    approval_id: 'apr_chat_work',
    task_id: 'tsk_chat_work',
    run_id: 'run_chat_work',
    worker_id: 'wrk_chat_work',
    claim_id: 'wcl_chat_work',
    host_instance_id: 'hst_chat_work',
    invocation_id: '0196ed00-0000-7000-8000-0000000005b1',
    sequence: 7,
    availability: 'live',
    freshness: 'stale',
    live: true,
    sampled_at_ms: 1_700_000_000_850,
    sample_age_ms: 5_250,
    max_sample_age_ms: 5_000,
    expires_at_ms: 1_700_000_000_900,
  };

  const snapshot = normalizeWorkspaceSnapshot({
    schema_version: 1,
    generated_at_ms: 1_700_000_001_000,
    chat_work: [
      {
        identity,
        availability: 'available',
        items: [
          doneItem,
          doingItem,
          { ...doneItem, item_id: 'blg_bad_status', status: 'mystery' },
          { ...doneItem, item_id: 'blg_bad_title', title: 'x'.repeat(257) },
        ],
        events: [failedEvent, failedEvent, completedEvent],
        worker_presence: [presence],
        bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
      },
      {
        identity: { ...identity, canonical_project_scope: 'relative/project' },
        availability: 'available',
        items: [doneItem],
        events: [],
        worker_presence: [],
        bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
      },
    ],
  });

  assert.equal(snapshot.chat_work.length, 1);
  const work = snapshot.chat_work[0]!;
  assert.deepEqual(work.identity, identity);
  assert.equal(work.availability, 'available');
  assert.equal(work.items.length, 2);
  assert.equal(work.items.find((item) => item.item_id === itemId)?.status, 'done');
  assert.equal(
    work.items.find((item) => item.item_id === doingItemId)?.status,
    'doing',
  );
  assert.equal(work.bounds.items_omitted, 2);
  assert.deepEqual(
    work.events.map((event) => event.sequence),
    [2, 10],
  );
  assert.equal(work.events[0]!.outcome, 'success');
  assert.equal(work.events[1]!.outcome, null);
  assert.ok(work.bounds.events_omitted >= 1);
  assert.equal(work.worker_presence.length, 1);
  assert.equal(work.worker_presence[0]!.live, false);
  assert.equal(work.worker_presence[0]!.freshness, 'stale');
  assert.equal(work.worker_presence[0]!.sampled_at_ms, 1_700_000_000_850);
  assert.equal(work.worker_presence[0]!.expires_at_ms, 1_700_000_000_900);

  const presenceItems = Array.from(
    { length: MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT + 3 },
    (_, index) => ({
      ...doingItem,
      item_id: `blg_chat_work_running_${index}`,
      current_execution_ref: `wbe_chat_work_running_${index}`,
      created_at_ms: 1_700_000_001_000 + index,
    }),
  );
  const tooManyPresence = normalizeWorkspaceChatWork({
    identity,
    availability: 'available',
    items: presenceItems,
    events: [],
    worker_presence: presenceItems.map((presenceItem, index) => ({
      ...presence,
      item_id: presenceItem.item_id,
      execution_ref: presenceItem.current_execution_ref,
      sequence: index + 1,
    })),
    bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
  });
  assert.ok(tooManyPresence);
  assert.equal(
    tooManyPresence.worker_presence.length,
    MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT,
  );
  assert.equal(tooManyPresence.bounds.presence_omitted, 3);

  const tooManyItems = normalizeWorkspaceChatWork({
    identity,
    availability: 'available',
    items: Array.from({ length: MAX_WORKSPACE_CHAT_WORK_ITEMS_PER_CHAT + 2 }, (_, index) => ({
      ...doneItem,
      item_id: `blg_chat_work_${index}`,
      created_at_ms: 1_700_000_000_100 + index,
    })),
    events: [],
    worker_presence: [],
    bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
  });
  assert.ok(tooManyItems);
  assert.equal(tooManyItems.items.length, MAX_WORKSPACE_CHAT_WORK_ITEMS_PER_CHAT);
  assert.equal(tooManyItems.bounds.items_omitted, 2);
});

test('chat_work worker executions default empty, join exact scope, dedupe by item/execution ref, and enforce 32/128 bounds', () => {
  const identity = sampleIdentity();
  const itemId = 'blg_worker_execution';
  const item = {
    item_id: itemId,
    binding_id: identity.binding_id,
    revision: 4,
    title: 'Run the verified attempt',
    acceptance_condition: 'The attempt is checked.',
    delivery_description: 'Deliver the checked attempt.',
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'The first attempt was not accepted.',
    current_execution_ref: 'wbe_2',
    completion_note: null,
    completion_evidence: null,
    completed_at_ms: null,
    created_at_ms: 1_700_000_000_100,
    updated_at_ms: 1_700_000_000_900,
  };
  const execution = (overrides: Record<string, unknown> = {}) => ({
    item_id: itemId,
    execution_ref: 'wbe_1',
    approval_id: 'apr_worker_execution',
    task_id: 'tsk_worker_execution',
    session_binding_id: identity.binding_id,
    run_id: 'run_worker_execution',
    worker_id: 'wrk_worker_execution',
    provider: 'opencode',
    requested_model: 'deepseek-v4.1-flash',
    observed_model: null,
    requested_effort: 'max',
    observed_effort: null,
    worker_status: 'completed',
    started_at_ms: 1_700_000_000_200,
    finished_at_ms: 1_700_000_000_700,
    duration_ms: 500,
    outcome: 'success',
    acceptance: 'rejected',
    ...overrides,
  });

  const legacy = normalizeWorkspaceChatWork({
    identity,
    availability: 'available',
    items: [item],
    events: [],
    worker_presence: [],
    bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
  });
  assert.ok(legacy);
  assert.deepEqual(legacy.worker_executions, []);
  assert.equal(legacy.bounds.executions_omitted, 0);

  const normalized = normalizeWorkspaceChatWork({
    identity,
    availability: 'available',
    items: [item],
    events: [],
    worker_presence: [],
    worker_executions: [
      // A refresh may repeat the same immutable attempt. The terminal record
      // wins so proven timing and metadata are never reset to a sparse copy.
      execution({ worker_status: 'running', outcome: null, duration_ms: null }),
      execution(),
      execution(),
      execution({ execution_ref: 'wbe_orphan', item_id: 'blg_missing' }),
      execution({
        execution_ref: 'wbe_runtime_binding',
        session_binding_id: 'bnd_runtime_approved',
        approval_id: 'apr_runtime',
        task_id: 'tsk_runtime',
        run_id: 'run_runtime',
        worker_id: 'wrk_runtime',
      }),
      execution({ execution_ref: 'wbe_cross_chat', item_id: 'blg_other_chat' }),
      execution({ execution_ref: 'wbe_bad_worker', worker_id: '' }),
      execution({
        execution_ref: 'wbe_2',
        worker_status: 'failed',
        outcome: 'failure',
        acceptance: null,
        started_at_ms: 1_700_000_000_800,
        finished_at_ms: 1_700_000_000_950,
        duration_ms: 150,
        provider: 'x'.repeat(300),
        observed_model: 'deepseek-v4.1-flash',
        observed_effort: 'max',
      }),
    ],
    bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
  });
  assert.ok(normalized);
  assert.deepEqual(
    normalized.worker_executions?.map((entry) => entry.execution_ref),
    ['wbe_1', 'wbe_runtime_binding', 'wbe_2'],
  );
  assert.equal(normalized.worker_executions?.[0]?.duration_ms, 500);
  assert.equal(normalized.worker_executions?.[0]?.finished_at_ms, 1_700_000_000_700);
  const runtimeBindingExecution = normalized.worker_executions?.find(
    (entry) => entry.execution_ref === 'wbe_runtime_binding',
  );
  assert.equal(runtimeBindingExecution?.session_binding_id, 'bnd_runtime_approved');
  assert.equal(runtimeBindingExecution?.approval_id, 'apr_runtime');
  assert.equal(runtimeBindingExecution?.task_id, 'tsk_runtime');
  assert.equal(runtimeBindingExecution?.run_id, 'run_runtime');
  assert.equal(runtimeBindingExecution?.worker_id, 'wrk_runtime');
  assert.equal(normalized.worker_executions?.[2]?.outcome, 'failure');
  assert.ok((normalized.worker_executions?.[2]?.provider?.length ?? 0) <= 128);
  assert.equal(normalized.worker_executions?.[2]?.observed_model, 'deepseek-v4.1-flash');
  assert.equal(normalized.worker_executions?.[2]?.observed_effort, 'max');
  assert.equal(normalized.bounds.executions_omitted, 3);

  const tooMany = normalizeWorkspaceChatWork({
    identity,
    availability: 'available',
    items: [item],
    events: [],
    worker_presence: [],
    worker_executions: Array.from(
      { length: MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_PER_CHAT + 2 },
      (_, index) =>
        execution({
          execution_ref: `wbe_cap_${index}`,
          started_at_ms: 1_700_000_000_200 + index,
          finished_at_ms: 1_700_000_000_700 + index,
        }),
    ),
    bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
  });
  assert.ok(tooMany);
  assert.equal(
    tooMany.worker_executions?.length,
    MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_PER_CHAT,
  );
  assert.equal(tooMany.bounds.executions_omitted, 2);

  const chats = Array.from({ length: 5 }, (_, chatIndex) => {
    const chatIdentity = validateWorkspaceChatIdentity({
      source_id: identity.source_id,
      binding_id: `bnd_execution_chat_${chatIndex}`,
      thread_uuid: VALID_UUID_A,
      canonical_project_scope: `/home/dev/repos/execution-${chatIndex}`,
    });
    const chatItem = {
      ...item,
      item_id: `blg_execution_chat_${chatIndex}`,
      binding_id: chatIdentity.binding_id,
      current_execution_ref: `wbe_chat_${chatIndex}_31`,
    };
    return {
      identity: chatIdentity,
      availability: 'available' as const,
      items: [chatItem],
      events: [],
      worker_presence: [],
      worker_executions: Array.from(
        { length: MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_PER_CHAT },
        (_, executionIndex) =>
          execution({
            item_id: chatItem.item_id,
            execution_ref: `wbe_chat_${chatIndex}_${executionIndex}`,
            session_binding_id: chatIdentity.binding_id,
            started_at_ms: 1_700_000_000_200 + executionIndex,
            finished_at_ms: 1_700_000_000_700 + executionIndex,
          }),
      ),
      bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
    };
  });
  const snapshot = normalizeWorkspaceSnapshot({
    schema_version: 1,
    generated_at_ms: 1_700_000_001_000,
    chat_work: chats,
  });
  const retainedExecutions = snapshot.chat_work.reduce(
    (sum, work) => sum + (work.worker_executions?.length ?? 0),
    0,
  );
  const omittedExecutions = snapshot.chat_work.reduce(
    (sum, work) => sum + (work.bounds.executions_omitted ?? 0),
    0,
  );
  assert.equal(retainedExecutions, MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_TOTAL);
  assert.equal(
    omittedExecutions,
    5 * MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_PER_CHAT -
      MAX_WORKSPACE_CHAT_WORK_EXECUTIONS_TOTAL,
  );
});

test('chat_work worker executions quarantine every conflicting immutable identity instead of ranking one row', () => {
  const identity = sampleIdentity();
  const itemId = 'blg_execution_identity';
  const item = {
    item_id: itemId,
    binding_id: identity.binding_id,
    revision: 5,
    title: 'Run the exact attempt',
    acceptance_condition: 'The attempt is checked.',
    delivery_description: 'Deliver the checked attempt.',
    kind: 'worker' as const,
    status: 'doing' as const,
    blocked_reason: null,
    current_execution_ref: 'wbe_identity',
    completion_note: null,
    completion_evidence: null,
    completed_at_ms: null,
    created_at_ms: 1_700_000_000_100,
    updated_at_ms: 1_700_000_000_900,
  };
  const execution = (overrides: Record<string, unknown> = {}) => ({
    item_id: itemId,
    execution_ref: 'wbe_identity',
    approval_id: 'apr_identity',
    task_id: 'tsk_identity',
    session_binding_id: 'bnd_runtime_identity',
    run_id: 'run_identity',
    worker_id: 'wrk_identity',
    provider: 'opencode',
    requested_model: 'deepseek-v4.1-flash',
    observed_model: null,
    requested_effort: 'max',
    observed_effort: null,
    worker_status: 'running',
    started_at_ms: 1_700_000_000_200,
    finished_at_ms: null,
    duration_ms: null,
    outcome: null,
    acceptance: null,
    ...overrides,
  });
  const normalize = (executions: Array<Record<string, unknown>>) => {
    const work = normalizeWorkspaceChatWork({
      identity,
      availability: 'available',
      items: [item],
      events: [],
      worker_presence: [],
      worker_executions: executions,
      bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
    });
    assert.ok(work);
    return work;
  };

  const running = execution();
  const terminal = execution({
    provider: 'google',
    requested_model: 'gemini-4-argon-high',
    observed_model: 'gemini-4-argon-high',
    worker_status: 'completed',
    finished_at_ms: 1_700_000_000_700,
    duration_ms: 500,
    outcome: 'success',
    acceptance: 'accepted',
  });
  const enriched = normalize([running, terminal, terminal]);
  assert.equal(enriched.worker_executions?.length, 1);
  assert.equal(enriched.worker_executions?.[0]?.worker_status, 'completed');
  assert.equal(enriched.worker_executions?.[0]?.duration_ms, 500);
  assert.equal(enriched.worker_executions?.[0]?.observed_model, 'gemini-4-argon-high');
  assert.equal(
    enriched.worker_executions?.[0]?.session_binding_id,
    'bnd_runtime_identity',
  );
  assert.notEqual(
    enriched.worker_executions?.[0]?.session_binding_id,
    identity.binding_id,
  );
  assert.equal(enriched.bounds.executions_omitted, 0);

  const conflictingFields: ReadonlyArray<[string, Record<string, unknown>]> = [
    ['approval_id', { approval_id: 'apr_other' }],
    ['task_id', { task_id: 'tsk_other' }],
    ['session_binding_id', { session_binding_id: 'bnd_runtime_other' }],
    ['run_id', { run_id: 'run_other' }],
    ['worker_id', { worker_id: 'wrk_other' }],
  ];
  for (const [field, override] of conflictingFields) {
    const conflicted = normalize([running, execution(override)]);
    assert.deepEqual(
      conflicted.worker_executions,
      [],
      `${field} contradiction must quarantine the whole same-ref group`,
    );
    assert.equal(
      conflicted.bounds.executions_omitted,
      2,
      `${field} contradiction must count both omitted rows`,
    );

    const resurrected = normalize([running, execution(override), terminal]);
    assert.deepEqual(
      resurrected.worker_executions,
      [],
      `${field} contradiction must not be resurrected by a later third row`,
    );
    assert.equal(
      resurrected.bounds.executions_omitted,
      3,
      `${field} contradiction must count the later omitted row`,
    );
  }
});

test('chat_work composition quarantines execution identity conflicts split across exact-identity entries', () => {
  const identity = sampleIdentity();
  const itemId = 'blg_cross_entry_identity';
  const item = {
    item_id: itemId,
    binding_id: identity.binding_id,
    revision: 6,
    title: 'Run the split attempt',
    acceptance_condition: 'The attempt is checked.',
    delivery_description: 'Deliver the split attempt.',
    kind: 'worker',
    status: 'doing',
    blocked_reason: null,
    current_execution_ref: 'wbe_cross_entry',
    completion_note: null,
    completion_evidence: null,
    completed_at_ms: null,
    created_at_ms: 1_700_000_000_100,
    updated_at_ms: 1_700_000_000_900,
  };
  const execution = (overrides: Record<string, unknown> = {}) => ({
    item_id: itemId,
    execution_ref: 'wbe_cross_entry',
    approval_id: 'apr_cross_entry',
    task_id: 'tsk_cross_entry',
    session_binding_id: 'bnd_runtime_cross_entry',
    run_id: 'run_cross_entry',
    worker_id: 'wrk_cross_entry',
    provider: 'opencode',
    requested_model: 'deepseek-v4.1-flash',
    observed_model: null,
    requested_effort: 'max',
    observed_effort: null,
    worker_status: 'running',
    started_at_ms: 1_700_000_000_200,
    finished_at_ms: null,
    duration_ms: null,
    outcome: null,
    acceptance: null,
    ...overrides,
  });
  const running = execution();
  const terminal = execution({
    provider: 'google',
    requested_model: 'gemini-4-argon-high',
    observed_model: 'gemini-4-argon-high',
    observed_effort: 'high',
    worker_status: 'completed',
    finished_at_ms: 1_700_000_000_700,
    duration_ms: 500,
    outcome: 'success',
    acceptance: 'accepted',
  });
  const workEntry = (
    workIdentity: WorkspaceChatIdentity,
    workItem: Record<string, unknown>,
    workerExecutions: Array<Record<string, unknown>>,
  ) => ({
    identity: workIdentity,
    availability: 'available' as const,
    items: [workItem],
    events: [],
    worker_presence: [],
    worker_executions: workerExecutions,
    bounds: {
      items_omitted: 0,
      events_omitted: 0,
      presence_omitted: 0,
      executions_omitted: 0,
    },
  });
  const snapshot = (chatWork: Array<Record<string, unknown>>) => ({
    schema_version: 1,
    generated_at_ms: 1_700_000_001_000,
    chat_work: chatWork,
  });
  const compositions = [
    ['normalizeWorkspaceSnapshot', (value: unknown) => normalizeWorkspaceSnapshot(value)],
    ['parseWorkspaceSnapshot', (value: unknown) => parseWorkspaceSnapshot(JSON.stringify(value))],
  ] as const;
  const conflictingFields: ReadonlyArray<[string, Record<string, unknown>]> = [
    ['approval_id', { approval_id: 'apr_cross_entry_other' }],
    ['task_id', { task_id: 'tsk_cross_entry_other' }],
    ['session_binding_id', { session_binding_id: 'bnd_runtime_cross_entry_other' }],
    ['run_id', { run_id: 'run_cross_entry_other' }],
    ['worker_id', { worker_id: 'wrk_cross_entry_other' }],
  ];

  for (const [field, override] of conflictingFields) {
    const conflicting = execution(override);
    for (const order of ['conflict_first', 'terminal_first'] as const) {
      const chatWork =
        order === 'conflict_first'
          ? [
              workEntry(identity, item, [running, conflicting]),
              workEntry(identity, item, [terminal]),
            ]
          : [
              workEntry(identity, item, [terminal]),
              workEntry(identity, item, [running, conflicting]),
            ];
      for (const [label, compose] of compositions) {
        const work = compose(snapshot(chatWork)).chat_work[0]!;
        assert.deepEqual(
          work.worker_executions,
          [],
          `${field} contradiction must quarantine the whole group across ${order} entries via ${label}`,
        );
        assert.equal(
          work.bounds.executions_omitted,
          3,
          `${field} must count every omitted raw row once via ${label}`,
        );
      }
    }
  }

  const conflictedSnapshot = snapshot([
    workEntry(identity, item, [running, execution({ approval_id: 'apr_cross_entry_other' })]),
    workEntry(identity, item, [terminal]),
  ]);
  for (const [label, compose] of compositions) {
    const composed = compose(conflictedSnapshot);
    const recomposed = compose(composed);
    const stable = recomposed.chat_work[0]!;
    assert.deepEqual(
      stable.worker_executions,
      [],
      `recomposing a quarantined chat must never resurrect an execution via ${label}`,
    );
    assert.equal(
      stable.bounds.executions_omitted,
      3,
      `recomposing a quarantined chat must not double-count or lose omissions via ${label}`,
    );
  }

  const otherIdentity = validateWorkspaceChatIdentity({
    source_id: identity.source_id,
    binding_id: 'bnd_cross_entry_other_chat',
    thread_uuid: VALID_UUID_B,
    canonical_project_scope: '/home/dev/repos/cross-entry-other',
  });
  const otherItem = {
    ...item,
    item_id: 'blg_cross_entry_other_chat',
    binding_id: otherIdentity.binding_id,
    current_execution_ref: 'wbe_cross_entry_other_chat',
  };
  const unrelatedExecution = execution({
    execution_ref: 'wbe_cross_entry_unrelated',
    approval_id: 'apr_cross_entry_unrelated',
    task_id: 'tsk_cross_entry_unrelated',
    run_id: 'run_cross_entry_unrelated',
    worker_id: 'wrk_cross_entry_unrelated',
  });
  const otherExecution = execution({
    item_id: otherItem.item_id,
    execution_ref: 'wbe_cross_entry_other_chat',
    approval_id: 'apr_cross_entry_other_chat',
    task_id: 'tsk_cross_entry_other_chat',
    session_binding_id: otherIdentity.binding_id,
    run_id: 'run_cross_entry_other_chat',
    worker_id: 'wrk_cross_entry_other_chat',
  });
  const mixedChatWork = [
    workEntry(identity, item, [running, execution({ approval_id: 'apr_cross_entry_other' })]),
    workEntry(identity, item, [terminal, unrelatedExecution]),
    workEntry(otherIdentity, otherItem, [otherExecution]),
  ];
  for (const [, compose] of compositions) {
    const normalized = compose(snapshot(mixedChatWork));
    const conflictedChat = normalized.chat_work.find((work) =>
      workspaceChatIdentitiesEqual(work.identity, identity),
    )!;
    assert.deepEqual(
      conflictedChat.worker_executions?.map((entry) => entry.execution_ref),
      ['wbe_cross_entry_unrelated'],
      'only the unrelated exact execution may survive the conflicted chat',
    );
    assert.equal(conflictedChat.bounds.executions_omitted, 3);
    const otherChat = normalized.chat_work.find((work) =>
      workspaceChatIdentitiesEqual(work.identity, otherIdentity),
    )!;
    assert.deepEqual(
      otherChat.worker_executions?.map((entry) => entry.execution_ref),
      ['wbe_cross_entry_other_chat'],
      'an independent chat keeps its unrelated valid execution',
    );
    assert.equal(otherChat.bounds.executions_omitted, 0);
  }

  for (const [label, compose] of compositions) {
    const enriched = compose(
      snapshot([workEntry(identity, item, [running]), workEntry(identity, item, [terminal])]),
    );
    assert.equal(
      enriched.chat_work[0]!.worker_executions?.length,
      1,
      `valid duplicate-chat enrichment must merge across entries via ${label}`,
    );
    assert.equal(enriched.chat_work[0]!.worker_executions?.[0]?.worker_status, 'completed');
    assert.equal(enriched.chat_work[0]!.worker_executions?.[0]?.duration_ms, 500);
    assert.equal(
      enriched.chat_work[0]!.worker_executions?.[0]?.observed_model,
      'gemini-4-argon-high',
    );
    assert.equal(
      enriched.chat_work[0]!.worker_executions?.[0]?.session_binding_id,
      'bnd_runtime_cross_entry',
    );
    assert.equal(enriched.chat_work[0]!.bounds.executions_omitted, 0);
  }
});

test('chat_work execution normalization keeps distinct runtime bindings and exact item/execution-ref history', () => {
  const identity = sampleIdentity();
  const sharedRef = 'wbe_shared';
  const baseItem = {
    binding_id: identity.binding_id,
    revision: 2,
    title: 'Run the isolated attempt',
    acceptance_condition: 'The attempt is checked.',
    delivery_description: 'Deliver the checked attempt.',
    kind: 'worker' as const,
    status: 'doing' as const,
    blocked_reason: null,
    current_execution_ref: sharedRef,
    completion_note: null,
    completion_evidence: null,
    completed_at_ms: null,
    created_at_ms: 1_700_000_000_100,
    updated_at_ms: 1_700_000_000_900,
  };
  const runtimeBindingId = 'bnd_runtime_approved';
  const execution = (overrides: Record<string, unknown> = {}) => ({
    item_id: 'blg_shared_a',
    execution_ref: sharedRef,
    approval_id: 'apr_shared',
    task_id: 'tsk_shared',
    session_binding_id: runtimeBindingId,
    run_id: 'run_shared',
    worker_id: 'wrk_shared',
    provider: 'opencode',
    requested_model: null,
    observed_model: null,
    requested_effort: null,
    observed_effort: null,
    worker_status: 'failed',
    started_at_ms: 1_700_000_000_200,
    finished_at_ms: 1_700_000_000_700,
    duration_ms: null,
    outcome: 'failure',
    acceptance: null,
    ...overrides,
  });
  const work = normalizeWorkspaceChatWork({
    identity,
    availability: 'available',
    items: [
      { ...baseItem, item_id: 'blg_shared_a' },
      {
        ...baseItem,
        item_id: 'blg_shared_b',
        status: 'blocked',
        blocked_reason: 'A different item reuses the same textual ref.',
      },
    ],
    events: [
      {
        event_id: 'blg_shared_a:2',
        sequence: 2,
        item_id: 'blg_shared_a',
        revision: 2,
        action: 'worker_outcome_recorded',
        recorded_at_ms: 1_700_000_000_700,
        title: 'Run the isolated attempt',
        status: 'doing',
        execution_ref: sharedRef,
        outcome: 'failure',
        note: null,
      },
      {
        event_id: 'blg_shared_b:2',
        sequence: 3,
        item_id: 'blg_shared_b',
        revision: 2,
        action: 'worker_outcome_recorded',
        recorded_at_ms: 1_700_000_000_800,
        title: 'Run the isolated attempt',
        status: 'blocked',
        execution_ref: sharedRef,
        outcome: null,
        note: null,
      },
      {
        event_id: 'blg_evicted:4',
        sequence: 4,
        item_id: 'blg_evicted',
        revision: 4,
        action: 'worker_outcome_recorded',
        recorded_at_ms: 1_700_000_000_900,
        title: 'Historical attempt',
        status: 'blocked',
        execution_ref: 'wbe_historical',
        outcome: 'failure',
        note: null,
      },
    ],
    worker_presence: [],
    worker_executions: [
      execution(),
      execution({
        item_id: 'blg_shared_b',
        started_at_ms: 1_700_000_000_300,
      }),
      execution({
        item_id: 'blg_evicted',
        execution_ref: 'wbe_historical',
        started_at_ms: 1_700_000_000_400,
      }),
      execution({
        item_id: 'blg_foreign',
        execution_ref: 'wbe_foreign',
        started_at_ms: 1_700_000_000_500,
      }),
    ],
    bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
  });

  assert.ok(work);
  assert.deepEqual(
    work.worker_executions?.map((entry) => [entry.item_id, entry.execution_ref]),
    [
      ['blg_shared_a', sharedRef],
      ['blg_shared_b', sharedRef],
      ['blg_evicted', 'wbe_historical'],
    ],
  );
  assert.ok(
    work.worker_executions?.every(
      (entry) => entry.session_binding_id === runtimeBindingId,
    ),
  );
  assert.equal(work.bounds.executions_omitted, 1);
});

test('legacy ObservationSnapshot and ObservationActor remain backward-compatible and sparse payloads degrade safely', () => {
  const actor: ObservationActor = {
    label: 'Codex · Alpha Chat',
    role: 'coworker',
    source: 'paired_mcp',
    model: null,
    effort: null,
    evidence: 'configured',
  };
  assert.equal(actor.model, null);
  assert.equal(actor.effort, null);
  assert.equal(actor.evidence, 'configured');

  const legacySnapshot: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 1_700_000_000_000,
    sources: [
      {
        id: 'default',
        label: 'alpha-app',
        project_path: '/home/dev/repos/alpha-app',
        availability: 'available',
        message: null,
      },
    ],
    jobs: [
      {
        source_id: 'default',
        id: 'job_legacy_01',
        project_label: 'alpha-app',
        project_path: '/home/dev/repos/alpha-app',
        title: 'Legacy Job',
        thread_uuid: VALID_UUID_A,
        thread_label: 'Legacy Thread Title',
        binding_id: 'bnd_chat_alpha',
        plan_id: 'plan_legacy_01',
        plan_revision: 2,
        state: 'running',
        approval_status: 'approved',
        execution_status: 'running',
        review_state: null,
        updated_at_ms: 1_700_000_000_000,
        active_call_count: 1,
        tasks: [
          {
            id: 'task-obs-main',
            title: 'Implement core',
            status: 'in_progress',
            depends_on: [],
          },
        ],
        summary: 'Running task',
      },
    ],
    requests: [
      {
        source_id: 'default',
        id: 'obsreq_legacy_01',
        job_id: 'job_legacy_01',
        parent_request_id: null,
        title: 'workflow get_plan',
        operation: 'mcp.get_plan',
        project_label: 'alpha-app',
        binding_id: 'bnd_chat_alpha',
        thread_uuid: VALID_UUID_A,
        thread_label: 'Legacy Thread Title',
        state: 'completed',
        started_at_ms: 1_700_000_000_000,
        finished_at_ms: 1_700_000_000_050,
        updated_at_ms: 1_700_000_000_050,
        duration_ms: 50,
        actor,
        executor: null,
        usage: null,
        delivery: [],
        children: [],
        children_count: 0,
        summary: 'Fetched active plan.',
      },
    ],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };

  // Bare legacy ObservationSnapshot JSON upgrades cleanly via parseWorkspaceSnapshot.
  const upgraded = parseWorkspaceSnapshot(JSON.stringify(legacySnapshot));
  assert.equal(upgraded.schema_version, 1);
  assert.deepEqual(upgraded.observation, legacySnapshot);
  assert.deepEqual(upgraded.chats[0]!.identity, sampleIdentity());
  assert.equal(upgraded.chats[0]!.availability, 'unavailable');
  assert.equal(upgraded.chats[0]!.activity, 'unknown');
  assert.equal(upgraded.chats[0]!.configured_selection.evidence, 'unavailable');
  assert.equal(upgraded.chats[0]!.configured_selection.counts_as_runtime_telemetry, false);
  assert.equal(upgraded.messages.availability, 'unavailable');
  assert.equal(upgraded.messages.requires_transcript, false);
  assert.equal(upgraded.messages.allows_synthetic_user_messages, false);
  assert.equal(upgraded.job_histories.length, 1);
  assert.equal(upgraded.job_histories[0]!.acceptance_proof.status, 'unknown');
  assert.equal(upgraded.job_histories[0]!.tasks[0]!.task_id, 'task-obs-main');

  assert.deepEqual(upgradeLegacyObservationSnapshot(legacySnapshot), upgraded);

  // Sparse / future enum values degrade safely to unknown / unavailable.
  const sparseJson = JSON.stringify({
    generated_at_ms: 12_345,
    sampling_policy: {
      snapshot_resets_sample_age: true,
    },
    chats: [
      {
        identity: {
          source_id: 'default',
          binding_id: 'bnd_chat_alpha',
          thread_uuid: VALID_UUID_A,
          canonical_project_scope: '/home/dev/repos/alpha-app',
        },
        availability: 'future_availability_variant',
        activity: 'future_activity_variant',
        configured_selection: {
          selected_model: 'gpt-5.4',
          evidence: 'unverified_runtime_claim',
          counts_as_runtime_telemetry: true,
        },
      },
    ],
    messages: {
      requires_transcript: true,
      allows_synthetic_user_messages: true,
    },
  });

  const sparse = parseWorkspaceSnapshot(sparseJson);
  assert.equal(sparse.sampling_policy.interval_ms, 2_000);
  assert.equal(sparse.sampling_policy.metadata_timeout_ms, 750);
  assert.equal(sparse.sampling_policy.stale_threshold_ms, 10_000);
  assert.equal(sparse.sampling_policy.snapshot_resets_sample_age, false);
  assert.equal(sparse.chats[0]!.availability, 'unavailable');
  assert.equal(sparse.chats[0]!.activity, 'unknown');
  assert.equal(sparse.chats[0]!.configured_selection.evidence, 'unavailable');
  assert.equal(sparse.chats[0]!.configured_selection.selected_model, null);
  assert.equal(sparse.chats[0]!.configured_selection.counts_as_runtime_telemetry, false);
  assert.equal(sparse.messages.availability, 'unavailable');
  assert.equal(sparse.messages.requires_transcript, false);
  assert.equal(sparse.messages.allows_synthetic_user_messages, false);
});

test('WorkspaceChatIdentity requires all four components and rejects titles, sessionId, and job_id substitutes', () => {
  const id = sampleIdentity();
  const expectedKey = formatWorkspaceChatIdentityKey(id);
  assert.ok(expectedKey !== null);

  // Renaming thread_label never alters canonical chat identity key.
  const chatOriginal = normalizeWorkspaceChatSummary({
    identity: id,
    thread_label: 'Initial Title',
    project_label: 'alpha-app',
    availability: 'available',
    activity: 'idle',
  });
  const chatRenamed = normalizeWorkspaceChatSummary({
    ...chatOriginal,
    thread_label: 'Completely Different Title',
  });
  assert.equal(
    formatWorkspaceChatIdentityKey(chatOriginal.identity),
    formatWorkspaceChatIdentityKey(chatRenamed.identity),
  );

  // Reject job_id, plan_id, sessionId, and human title substitutes.
  for (const badBinding of [
    'job_0196ed',
    'job-123',
    'plan_456',
    'sess_abc',
    'session_xyz',
    'sessionId_123',
    'My Chat Title With Spaces',
    '',
  ]) {
    assert.throws(
      () =>
        validateWorkspaceChatIdentity({
          source_id: 'default',
          binding_id: badBinding,
          thread_uuid: VALID_UUID_A,
          canonical_project_scope: '/home/dev/repos/alpha-app',
        }),
      (err: unknown) => err instanceof WorkspaceContractError && err.code === 'invalid_chat_identity',
    );
  }

  for (const badSource of ['job_source', 'session_source', 'source with space', '']) {
    assert.throws(
      () =>
        validateWorkspaceChatIdentity({
          source_id: badSource,
          binding_id: 'bnd_ok',
          thread_uuid: VALID_UUID_A,
          canonical_project_scope: '/home/dev/repos/alpha-app',
        }),
      (err: unknown) => err instanceof WorkspaceContractError && err.code === 'invalid_chat_identity',
    );
  }

  // Reject nil, uppercase, or non-UUID thread_uuid values.
  for (const badUuid of [
    '00000000-0000-0000-0000-000000000000',
    '0196ED00-0000-7000-8000-000000000042',
    'sess_0196ed00',
    'job_0196ed00',
    'not-a-uuid',
  ]) {
    assert.throws(
      () =>
        validateWorkspaceChatIdentity({
          source_id: 'default',
          binding_id: 'bnd_ok',
          thread_uuid: badUuid,
          canonical_project_scope: '/home/dev/repos/alpha-app',
        }),
      (err: unknown) => err instanceof WorkspaceContractError && err.code === 'invalid_chat_identity',
    );
  }

  // Reject relative, unnormalized, or trailing-slash project paths.
  for (const badPath of [
    'relative/path',
    '/home//dev/repo',
    '/home/dev/./repo',
    '/home/dev/../repo',
    '/home/dev/repo/',
    '',
  ]) {
    assert.throws(
      () =>
        validateWorkspaceChatIdentity({
          source_id: 'default',
          binding_id: 'bnd_ok',
          thread_uuid: VALID_UUID_A,
          canonical_project_scope: badPath,
        }),
      (err: unknown) => err instanceof WorkspaceContractError && err.code === 'invalid_chat_identity',
    );
  }

  // Distinct canonical_project_scope produces a distinct canonical key.
  const worktreeId = validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_chat_alpha',
    thread_uuid: VALID_UUID_A,
    canonical_project_scope: '/home/dev/repos/alpha-app-wt2',
  });
  assert.notEqual(formatWorkspaceChatIdentityKey(id), formatWorkspaceChatIdentityKey(worktreeId));
});

test('native sample producer_epoch, ObservationProcessInstance, and monotonic sequence reconcile deterministically', () => {
  const id = sampleIdentity();
  const proc1 = sampleProcessInstance(4100, 987654);
  const s1 = sampleNativeLive(id, 'epoch_01', proc1, 1, 10_000);
  assert.equal(reconcileWorkspaceNativeSample(null, s1), 'accepted_initial');

  const s2 = sampleNativeLive(id, 'epoch_01', proc1, 2, 12_000);
  assert.equal(reconcileWorkspaceNativeSample(s1, s2), 'accepted_next_sequence');

  const dup = sampleNativeLive(id, 'epoch_01', proc1, 2, 12_500);
  assert.equal(reconcileWorkspaceNativeSample(s2, dup), 'rejected_duplicate_sequence');

  const staleSeq = sampleNativeLive(id, 'epoch_01', proc1, 1, 13_000);
  assert.equal(reconcileWorkspaceNativeSample(s2, staleSeq), 'rejected_stale_sequence');

  const procSpoof = sampleProcessInstance(4100, 999999);
  const badEpochReuse = sampleNativeLive(id, 'epoch_01', procSpoof, 3, 14_000);
  assert.equal(
    reconcileWorkspaceNativeSample(s2, badEpochReuse),
    'rejected_epoch_process_mismatch',
  );

  const proc2 = sampleProcessInstance(5200, 1050000);
  const oldRestart = sampleNativeLive(id, 'epoch_02', proc2, 1, 11_000);
  assert.equal(reconcileWorkspaceNativeSample(s2, oldRestart), 'rejected_stale_epoch');

  const validRestart = sampleNativeLive(id, 'epoch_02', proc2, 1, 14_000);
  assert.equal(reconcileWorkspaceNativeSample(s2, validRestart), 'accepted_new_epoch');

  const otherId = validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_other',
    thread_uuid: VALID_UUID_B,
    canonical_project_scope: '/home/dev/repos/alpha-app',
  });
  const mismatchedSample = sampleNativeLive(otherId, 'epoch_02', proc2, 2, 15_000);
  assert.equal(
    reconcileWorkspaceNativeSample(validRestart, mismatchedSample),
    'rejected_mismatched_identity',
  );
});

test('availability states stay separated from activity detail and selected_model/effort are configuration evidence only', () => {
  const id = sampleIdentity();
  for (const activity of ['working', 'idle', 'waiting_for_approval', 'waiting_for_input', 'unknown'] as const) {
    const sample: WorkspaceNativeSample = {
      ...sampleNativeLive(id, 'epoch_01', sampleProcessInstance(10, 20), 1, 10_000),
      activity,
    };
    const evaluated = evaluateWorkspaceNativeSampleAt(sample, 12_000, 10_000);
    assert.equal(evaluated.effective_availability, 'available');
    assert.equal(evaluated.effective_activity, activity);
    assert.equal(evaluated.effective_configured_selection.selected_model, 'gpt-5.4');
    assert.equal(evaluated.effective_configured_selection.selected_effort, 'high');
    assert.equal(evaluated.effective_configured_selection.evidence, 'verified_configuration');
    assert.equal(evaluated.effective_configured_selection.counts_as_runtime_telemetry, false);
  }

  for (const nonAvailable of ['disconnected', 'stale', 'unavailable'] as const) {
    const norm = normalizeWorkspaceNativeSample({
      ...sampleNativeLive(id, 'epoch_01', sampleProcessInstance(10, 20), 2, 10_000),
      availability: nonAvailable,
      activity: 'working',
    });
    assert.equal(norm.availability, nonAvailable);
    assert.equal(norm.activity, 'unknown');
    assert.equal(norm.configured_selection.evidence, 'unavailable');
    assert.equal(norm.configured_selection.selected_model, null);
    assert.equal(norm.configured_selection.counts_as_runtime_telemetry, false);
  }
});

test('sampling policy enforces 2s/750ms/10s defaults and regenerateWorkspaceSnapshotAt never resets sample age', () => {
  assert.equal(WORKSPACE_SAMPLING_INTERVAL_MS, 2_000);
  assert.equal(WORKSPACE_METADATA_TIMEOUT_MS, 750);
  assert.equal(WORKSPACE_SAMPLE_STALE_THRESHOLD_MS, 10_000);
  assert.deepEqual(DEFAULT_WORKSPACE_SAMPLING_POLICY, {
    interval_ms: 2_000,
    metadata_timeout_ms: 750,
    stale_threshold_ms: 10_000,
    snapshot_resets_sample_age: false,
  });

  const id = sampleIdentity();
  const sample = sampleNativeLive(id, 'epoch_age', sampleProcessInstance(77, 88), 5, 50_000);
  const snapFresh = normalizeWorkspaceSnapshot({
    generated_at_ms: 55_000,
    native_samples: [sample],
  });
  assert.equal(snapFresh.chats[0]!.availability, 'available');
  assert.equal(snapFresh.chats[0]!.activity, 'working');
  assert.equal(snapFresh.native_samples[0]!.observed_at_ms, 50_000);
  assert.equal(snapFresh.native_samples[0]!.last_success_at_ms, 50_000);

  // At exact 10,000 ms boundary, sample remains fresh.
  const snapBoundary = regenerateWorkspaceSnapshotAt(snapFresh, 60_000);
  assert.equal(snapBoundary.generated_at_ms, 60_000);
  assert.equal(snapBoundary.native_samples[0]!.observed_at_ms, 50_000);
  assert.equal(snapBoundary.native_samples[0]!.last_success_at_ms, 50_000);
  assert.equal(snapBoundary.chats[0]!.availability, 'available');
  assert.equal(snapBoundary.chats[0]!.activity, 'working');

  // At 10,001 ms, sample transitions to stale without mutating sample timestamps.
  const snapStale = regenerateWorkspaceSnapshotAt(snapBoundary, 60_001);
  assert.equal(snapStale.generated_at_ms, 60_001);
  assert.equal(snapStale.native_samples[0]!.observed_at_ms, 50_000);
  assert.equal(snapStale.native_samples[0]!.last_success_at_ms, 50_000);
  assert.equal(snapStale.chats[0]!.availability, 'stale');
  assert.equal(snapStale.chats[0]!.activity, 'unknown');
  assert.equal(snapStale.chats[0]!.configured_selection.evidence, 'unavailable');
});

test('native sample acquisition ticks never become durable chat recency', () => {
  const sampleOnlyId = validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_chat_beta',
    thread_uuid: VALID_UUID_B,
    canonical_project_scope: '/home/dev/repos/alpha-app',
  });
  const process = sampleProcessInstance(77, 88);
  const firstSample: WorkspaceNativeSample = {
    ...sampleNativeLive(sampleOnlyId, 'epoch_recency_01', process, 1, 50_000),
    activity: 'idle',
  };

  const sampleOnly = normalizeWorkspaceSnapshot({
    generated_at_ms: 55_000,
    native_samples: [firstSample],
  });
  assert.equal(sampleOnly.chats.length, 1);
  assert.equal(sampleOnly.chats[0]!.availability, 'available');
  assert.equal(sampleOnly.chats[0]!.activity, 'idle');
  assert.equal(sampleOnly.chats[0]!.updated_at_ms, null);

  const laterTick = normalizeWorkspaceSnapshot({
    generated_at_ms: 60_000,
    native_samples: [
      {
        ...firstSample,
        sequence: 2,
        observed_at_ms: 59_000,
        last_success_at_ms: 59_000,
        activity: 'waiting_for_input',
      },
    ],
  });
  assert.equal(laterTick.chats[0]!.activity, 'waiting_for_input');
  assert.equal(laterTick.chats[0]!.updated_at_ms, null);
  assert.equal(
    regenerateWorkspaceSnapshotAt(laterTick, 70_000).chats[0]!.updated_at_ms,
    null,
  );

  const durableMs = 40_000;
  const durableChat = {
    identity: sampleOnlyId,
    thread_label: 'Durable chat',
    project_label: 'alpha-app',
    availability: 'available' as const,
    activity: 'idle' as const,
    configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
    active_job_count: 0,
    attention_count: 0,
    updated_at_ms: durableMs,
  };
  const durable = normalizeWorkspaceSnapshot({
    generated_at_ms: 55_000,
    chats: [durableChat],
    native_samples: [firstSample],
  });
  assert.equal(durable.chats[0]!.updated_at_ms, durableMs);

  const durableAfterNewerTick = normalizeWorkspaceSnapshot({
    generated_at_ms: 60_000,
    chats: [durableChat],
    native_samples: [
      {
        ...firstSample,
        sequence: 3,
        observed_at_ms: 59_500,
        last_success_at_ms: 59_500,
        activity: 'working',
      },
    ],
  });
  assert.equal(durableAfterNewerTick.chats[0]!.updated_at_ms, durableMs);
});

test('job history separates attempts/runs/outbox/candidates/links/failure/attention and requires receipt proof for verified_accepted', () => {
  const unbacked = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_hist_01',
      binding_id: 'bnd_chat_alpha',
      chat_identity: sampleIdentity(),
      title: 'Implement contract',
      job_revision: 7,
      state: 'reviewing',
      phase: 'closing_review',
      approval_status: 'approved',
      execution_status: 'completed',
      review_state: 'pending',
      created_at_ms: 1_000,
      updated_at_ms: 5_000,
      summary: 'Worker completed implementation.',
    },
    tasks: [
      {
        task_id: 'task_impl',
        title: 'Write Rust & TS types',
        role: 'coder',
        difficulty: 'hard',
        stage: 'review_pending',
        status: 'completed',
        depends_on: [],
        current_attempt_no: 2,
        attempts: [
          {
            attempt_no: 1,
            status: 'failed',
            run_id: 'run_01',
            worker_id: 'w_01',
            outbox_id: 'outbox_01',
            executor: null,
            started_at_ms: 1_100,
            finished_at_ms: 2_000,
            summary: 'Timed out on metadata.',
          },
          {
            attempt_no: 2,
            status: 'completed',
            run_id: 'run_02',
            worker_id: 'w_02',
            outbox_id: 'outbox_02',
            executor: null,
            started_at_ms: 2_500,
            finished_at_ms: 4_500,
            summary: 'Completed candidate.',
          },
        ],
        candidates: [
          {
            candidate_id: 'cand_02',
            attempt_no: 2,
            worker_id: 'w_02',
            head_commit_sha: 'abc12345',
            tree_sha: 'def67890',
            verification_passed: true,
            review_status: 'pending',
            review_verdict_id: null,
            review_receipt_id: null,
            produced_at_ms: 4_500,
            summary: 'Candidate ready for review.',
          },
        ],
        accepted_candidate_id: 'cand_02',
        historical_failure: {
          failure_id: 'fail_01',
          stage: 'worker_execution',
          failed_at_ms: 2_000,
          task_id: 'task_impl',
          attempt_no: 1,
          run_id: 'run_01',
          outbox_id: 'outbox_01',
          reason: 'Attempt 1 timed out.',
          superseded_or_resolved: true,
        },
        current_attention: {
          kind: 'review_required',
          reason: 'Awaiting Reviewer verdict.',
          next_step: 'Run Reviewer role.',
          blocking_hold_ids: [],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 4_600,
        },
        acceptance_proof: {
          status: 'verified_accepted',
          accepted_candidate_id: 'cand_02',
          // Missing review_verdict_id and review_receipt_id!
        },
      },
    ],
    runs: [
      {
        run_id: 'run_02',
        worker_id: 'w_02',
        binding_id: 'bnd_chat_alpha',
        task_id: 'task_impl',
        attempt_no: 2,
        role: 'coder',
        status: 'completed',
        executor: null,
        started_at_ms: 2_500,
        finished_at_ms: 4_500,
        updated_at_ms: 4_500,
      },
    ],
    outbox: [
      {
        outbox_id: 'outbox_02',
        request_key: 'req_key_02',
        kind: 'worker_dispatch',
        status: 'reconciled',
        task_id: 'task_impl',
        attempt_no: 2,
        role: 'coder',
        run_id: 'run_02',
        worker_id: 'w_02',
        receipt_id: 'rcpt_02',
        reserved_at_job_revision: 5,
        updated_at_job_revision: 6,
        diagnostic: null,
      },
    ],
    combined_candidates: [
      {
        combined_candidate_id: 'ccand_01',
        job_revision: 7,
        status: 'pending_closure',
        final_commit_sha: 'abc12345',
        final_tree_sha: 'def67890',
        candidate_digest: 'sha256_digest_01',
        closure_reservation_id: 'clos_res_01',
        check_count: 4,
        produced_at_ms: 4_600,
      },
    ],
    result: {
      outcome: 'ready_for_review',
      summary: 'Worker completed; awaiting Closure verdict.',
      produced_at_ms: 4_600,
      delivery_stage: 'result_ready',
      closure_verdict_id: null,
    },
    links: {
      source_plan_id: 'plan_01',
      source_plan_revision: 3,
      source_approval_id: 'apr_01',
      source_work_brief_id: 'brief_01',
      planning_grant_id: 'pgrant_01',
      planning_reservation_id: 'pres_01',
      supersedes_job_id: 'job_prev_00',
      superseded_by_job_id: null,
      parent_job_id: 'job_parent_00',
      follow_up_job_ids: ['job_followup_02'],
      architect_advice_ids: ['adv_01'],
      source_request_ids: ['req_01', 'req_02'],
    },
    historical_failure: {
      failure_id: 'fail_01',
      stage: 'worker_execution',
      failed_at_ms: 2_000,
      task_id: 'task_impl',
      attempt_no: 1,
      run_id: 'run_01',
      outbox_id: 'outbox_01',
      reason: 'Attempt 1 failed prior to retry.',
      superseded_or_resolved: true,
    },
    current_attention: {
      kind: 'review_required',
      reason: 'Awaiting Reviewer and Closure verdicts.',
      next_step: 'Run closing review.',
      blocking_hold_ids: [],
      blocking_outbox_ids: [],
      question_id: null,
      updated_at_ms: 4_700,
    },
    acceptance_proof: {
      status: 'verified_accepted',
      combined_candidate_id: 'ccand_01',
      // Missing closure_verdict_id!
    },
  });

  assert.equal(unbacked.tasks[0]!.current_attempt_no, 2);
  assert.equal(unbacked.tasks[0]!.attempts.length, 2);
  assert.equal(unbacked.runs.length, 1);
  assert.equal(unbacked.outbox.length, 1);
  assert.equal(unbacked.combined_candidates.length, 1);
  assert.deepEqual(unbacked.links.follow_up_job_ids, ['job_followup_02']);
  assert.equal(unbacked.historical_failure?.superseded_or_resolved, true);
  assert.equal(unbacked.current_attention?.kind, 'review_required');
  // Downgraded because review/closure verdict proofs are absent.
  assert.equal(unbacked.tasks[0]!.acceptance_proof.status, 'unknown');
  assert.equal(unbacked.acceptance_proof.status, 'unknown');

  // Providing verified review and closure verdict IDs preserves `verified_accepted`.
  const backed = normalizeWorkspaceJobHistory({
    ...unbacked,
    tasks: [
      {
        ...unbacked.tasks[0]!,
        acceptance_proof: {
          ...unbacked.tasks[0]!.acceptance_proof,
          status: 'verified_accepted',
          review_verdict_id: 'rverd_01',
          verified_at_ms: 4_800,
        },
      },
    ],
    acceptance_proof: {
      ...unbacked.acceptance_proof,
      status: 'verified_accepted',
      closure_verdict_id: 'cverd_01',
      verified_at_ms: 4_900,
    },
  });
  assert.equal(backed.tasks[0]!.acceptance_proof.status, 'verified_accepted');
  assert.equal(backed.acceptance_proof.status, 'verified_accepted');
});

test('external resolution view contract is ledger-independent, backward-compatible, store-verified, and bounded', () => {
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION,
    'workspace_external_resolution_view_v1',
  );
  assert.deepEqual(WORKSPACE_EXTERNAL_RESOLUTION_OUTCOMES, ['resolved_externally']);
  assert.deepEqual(WORKSPACE_EXTERNAL_RESOLUTION_SCOPES, ['job', 'tasks']);
  assert.deepEqual(WORKSPACE_EXTERNAL_RESOLUTION_APPLICABILITIES, [
    'applicable',
    'stale',
    'unverified',
  ]);
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.version,
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.max_external_resolutions_per_job,
    MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.max_external_resolutions_per_job,
    MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.max_summary_chars,
    MAX_OBSERVATION_SUMMARY_CHARS,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.requires_store_verified_applicability,
    true,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.forbids_frontend_proof_synthesis,
    true,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.preserves_covered_attention_rows,
    true,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.full_job_label,
    'Resolved externally',
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.partial_tasks_label,
    'Partially resolved externally',
  );

  const baseJob = {
    source_id: 'default',
    job_id: 'job_ext_contract_01',
    binding_id: 'bnd_chat_alpha',
    chat_identity: sampleIdentity(),
    title: 'External contract job',
    state: 'completed',
    approval_status: 'approved',
    execution_status: 'completed',
  };

  const absent = normalizeWorkspaceJobHistory({ job: baseJob });
  assert.deepEqual(absent.external_resolutions, []);
  assert.equal(absent.truncation.external_resolutions_omitted, undefined);

  for (const raw of [
    undefined,
    null,
    {},
    'not-a-list',
    [null, 42, {}, { receipt_id: 'bad id', outcome: 'resolved_externally', scope: 'job' }],
  ]) {
    assert.deepEqual(normalizeWorkspaceExternalResolutions(raw), []);
  }

  const full = normalizeWorkspaceExternalResolution({
    receipt_id: 'rcpt_contract_01',
    request_key: 'req_contract_01',
    preview_digest: 'digest_contract_01',
    outcome: 'resolved_externally',
    scope: 'job',
    task_ids: [],
    applicability: 'applicable',
    summary: 'Resolved externally by the operator.',
    evidence_refs: ['ev_contract_01'],
    recorded_at_ms: 1_000,
    covered_attention: [
      {
        task_id: null,
        attention: {
          kind: 'needs_approval',
          reason: 'Approval covered by the external receipt.',
          next_step: 'No further approval is required.',
          blocking_hold_ids: ['hold_contract_01'],
          blocking_outbox_ids: ['outbox_contract_01'],
          question_id: 'q_contract_01',
          updated_at_ms: 1_000,
        },
      },
    ],
  });
  assert.ok(full);
  assert.equal(full.outcome, 'resolved_externally');
  assert.equal(full.scope, 'job');
  assert.equal(full.applicability, 'applicable');
  assert.equal(full.covered_attention.length, 1);
  assert.equal(full.covered_attention[0]?.task_id, null);
  assert.deepEqual(full.covered_attention[0]?.attention.blocking_hold_ids, [
    'hold_contract_01',
  ]);

  const missingApplicability = normalizeWorkspaceExternalResolution({
    ...full,
    applicability: undefined,
  });
  assert.equal(missingApplicability?.applicability, 'unverified');

  const tasksWithoutValidIds = normalizeWorkspaceExternalResolution({
    ...full,
    scope: 'tasks',
    task_ids: ['unknown', 'bad id'],
  });
  assert.equal(tasksWithoutValidIds?.applicability, 'unverified');

  assert.equal(
    normalizeWorkspaceExternalResolution({ ...full, outcome: 'completed' }),
    null,
  );
  assert.equal(
    normalizeWorkspaceExternalResolution({ ...full, scope: 'thread' }),
    null,
  );
  assert.equal(
    normalizeWorkspaceExternalResolution({ ...full, receipt_id: 'bad id' }),
    null,
  );

  const malformed = normalizeWorkspaceExternalResolution({
    ...full,
    request_key: 'bad key!',
    preview_digest: 'bad digest!',
    task_ids: ['task_1', 'bad id', 'task_1'],
    evidence_refs: ['ev_1', 'bad ref', 'ev_1'],
    summary: 'X'.repeat(260),
    covered_attention: [
      {
        task_id: 'task_1',
        attention: {
          kind: 'on_hold',
          reason: 'Held task.',
          next_step: null,
          blocking_hold_ids: ['hold_1'],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      },
      {
        task_id: 'bad id',
        attention: {
          kind: 'unknown',
          reason: 'Unknown task.',
          next_step: null,
          blocking_hold_ids: [],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      },
      { task_id: 'task_2', attention: null },
      {
        task_id: 'task_3',
        attention: {
          kind: 'needs_user_input',
          reason: 'Question covered.',
          next_step: 'Answer in Codex.',
          blocking_hold_ids: [],
          blocking_outbox_ids: [],
          question_id: 'q_1',
          updated_at_ms: 1_000,
        },
      },
    ],
  });
  assert.ok(malformed);
  assert.equal(malformed.request_key, null);
  assert.equal(malformed.preview_digest, null);
  assert.deepEqual(malformed.task_ids, ['task_1']);
  assert.deepEqual(malformed.evidence_refs, ['ev_1']);
  assert.equal(Array.from(malformed.summary).length, MAX_OBSERVATION_SUMMARY_CHARS);
  assert.equal(malformed.covered_attention.length, 1);
  assert.equal(malformed.covered_attention[0]?.task_id, 'task_1');
  assert.equal(
    malformed.covered_attention.some(
      (covered) => covered.task_id === null || covered.task_id === 'task_3',
    ),
    false,
  );

  const partialMalformedCoverage = normalizeWorkspaceExternalResolution({
    receipt_id: 'rcpt_contract_partial_malformed',
    outcome: 'resolved_externally',
    scope: 'tasks',
    task_ids: ['task_a'],
    applicability: 'applicable',
    summary: 'Partial receipt with a malformed covered task identity.',
    covered_attention: [
      {
        task_id: 'task_a',
        attention: {
          kind: 'on_hold',
          reason: 'Task A is covered.',
          next_step: null,
          blocking_hold_ids: ['hold_a'],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      },
      {
        task_id: 'bad id',
        attention: {
          kind: 'unknown',
          reason: 'Malformed identity must not become job-level coverage.',
          next_step: null,
          blocking_hold_ids: [],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      },
      {
        task_id: 'task_b',
        attention: {
          kind: 'needs_approval',
          reason: 'Undeclared task B must not be invented as covered.',
          next_step: null,
          blocking_hold_ids: ['hold_b'],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      },
    ],
  });
  assert.ok(partialMalformedCoverage);
  assert.deepEqual(partialMalformedCoverage.task_ids, ['task_a']);
  assert.equal(partialMalformedCoverage.covered_attention.length, 1);
  assert.equal(partialMalformedCoverage.covered_attention[0]?.task_id, 'task_a');

  const explicitJobLevelCoverage = normalizeWorkspaceExternalResolution({
    ...partialMalformedCoverage,
    receipt_id: 'rcpt_contract_explicit_job_level',
    covered_attention: [
      {
        task_id: null,
        attention: {
          kind: 'needs_approval',
          reason: 'Explicit job-level coverage remains.',
          next_step: null,
          blocking_hold_ids: ['hold_job'],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      },
      {
        task_id: 'bad id',
        attention: {
          kind: 'unknown',
          reason: 'Malformed identity is still excluded.',
          next_step: null,
          blocking_hold_ids: [],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      },
    ],
  });
  assert.ok(explicitJobLevelCoverage);
  assert.equal(explicitJobLevelCoverage.covered_attention.length, 1);
  assert.equal(explicitJobLevelCoverage.covered_attention[0]?.task_id, null);

  const many: WorkspaceExternalResolution[] = Array.from(
    { length: MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB + 3 },
    (_, idx) => ({
      ...full,
      receipt_id: `rcpt_contract_${String(idx).padStart(3, '0')}`,
      recorded_at_ms: 1_000 + idx,
    }),
  );
  const boundedHistory = normalizeWorkspaceJobHistory({
    job: baseJob,
    external_resolutions: many,
  });
  assert.equal(
    boundedHistory.external_resolutions?.length,
    MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB,
  );
  assert.equal(boundedHistory.truncation.external_resolutions_omitted, 3);
  assert.equal(
    boundedHistory.external_resolutions?.[0]?.receipt_id,
    'rcpt_contract_003',
  );

  const duplicateHistory = normalizeWorkspaceJobHistory({
    job: baseJob,
    external_resolutions: [
      { ...full, receipt_id: 'rcpt_duplicate_01', summary: 'Older receipt.', recorded_at_ms: 1_000 },
      { ...full, receipt_id: 'rcpt_duplicate_01', summary: 'Newer receipt.', recorded_at_ms: 2_000 },
    ],
  });
  assert.equal(duplicateHistory.external_resolutions?.length, 1);
  assert.equal(duplicateHistory.external_resolutions?.[0]?.summary, 'Newer receipt.');
  assert.equal(duplicateHistory.truncation.external_resolutions_omitted, 1);

  const oversized = normalizeWorkspaceExternalResolution({
    ...full,
    task_ids: Array.from(
      { length: MAX_WORKSPACE_TASKS_PER_JOB + 5 },
      (_, idx) => `task_${idx}`,
    ),
    evidence_refs: Array.from(
      { length: MAX_WORKSPACE_RECORD_LINKS_PER_LIST + 5 },
      (_, idx) => `ev_${idx}`,
    ),
    covered_attention: Array.from(
      { length: MAX_WORKSPACE_RECORD_LINKS_PER_LIST + 5 },
      (_, idx) => ({
        task_id: `task_${idx}`,
        attention: {
          kind: 'on_hold',
          reason: `Covered ${idx}.`,
          next_step: null,
          blocking_hold_ids: [],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: 1_000,
        },
      }),
    ),
  });
  assert.equal(oversized?.task_ids.length, MAX_WORKSPACE_TASKS_PER_JOB);
  assert.equal(
    oversized?.evidence_refs.length,
    MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
  );
  assert.equal(
    oversized?.covered_attention.length,
    MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
  );

  const legacyObservation: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 1_000,
    sources: [],
    jobs: [
      {
        source_id: 'default',
        id: 'job_legacy_external_01',
        project_label: 'Legacy',
        project_path: null,
        title: 'Legacy job',
        thread_uuid: null,
        thread_label: null,
        binding_id: null,
        plan_id: null,
        plan_revision: null,
        state: 'completed',
        approval_status: 'approved',
        execution_status: 'completed',
        review_state: null,
        updated_at_ms: 1_000,
        active_call_count: 0,
        tasks: [],
        summary: 'Legacy snapshot without external resolutions.',
      },
    ],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };
  const upgraded = upgradeLegacyObservationSnapshot(legacyObservation);
  assert.deepEqual(upgraded.job_histories[0]?.external_resolutions, []);
});

test('external resolution covered attention retains the job row plus maximum task rows and stays bounded', () => {
  assert.equal(MAX_WORKSPACE_TASKS_PER_JOB, 64);
  assert.equal(MAX_WORKSPACE_RECORD_LINKS_PER_LIST, 64);
  assert.equal(
    MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION,
    MAX_WORKSPACE_TASKS_PER_JOB + 1,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.max_covered_attention_per_resolution,
    MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.max_task_ids_per_resolution,
    MAX_WORKSPACE_TASKS_PER_JOB,
  );
  assert.equal(
    WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT.max_evidence_refs_per_resolution,
    MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
  );

  const attention = {
    kind: 'needs_approval',
    reason: 'Covered by an external receipt.',
    next_step: null,
    blocking_hold_ids: ['hold_covered_01'],
    blocking_outbox_ids: [],
    question_id: null,
    updated_at_ms: 1_000,
  };
  const taskIds = Array.from({ length: MAX_WORKSPACE_TASKS_PER_JOB }, (_, idx) =>
    `task_covered_${String(idx).padStart(2, '0')}`,
  );
  const maxCovered = [
    { task_id: null, attention },
    ...taskIds.map((taskId) => ({ task_id: taskId, attention })),
  ];
  const maxResolution = normalizeWorkspaceExternalResolution({
    receipt_id: 'rcpt_covered_max_01',
    outcome: 'resolved_externally',
    scope: 'job',
    task_ids: taskIds,
    applicability: 'applicable',
    summary: 'Maximum covered attention.',
    evidence_refs: Array.from(
      { length: MAX_WORKSPACE_RECORD_LINKS_PER_LIST + 5 },
      (_, idx) => `ev_covered_${idx}`,
    ),
    covered_attention: maxCovered,
  });
  assert.ok(maxResolution);
  assert.equal(maxResolution.task_ids.length, MAX_WORKSPACE_TASKS_PER_JOB);
  assert.equal(maxResolution.evidence_refs.length, MAX_WORKSPACE_RECORD_LINKS_PER_LIST);
  assert.equal(
    maxResolution.covered_attention.length,
    MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION,
  );
  assert.equal(maxResolution.covered_attention[0]?.task_id, null);
  assert.deepEqual(
    maxResolution.covered_attention.slice(1).map((covered) => covered.task_id),
    taskIds,
  );
  assert.deepEqual(normalizeWorkspaceExternalResolution(maxResolution), maxResolution);

  const overflowing = normalizeWorkspaceExternalResolution({
    ...maxResolution,
    covered_attention: [
      ...Array.from(
        { length: MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION + 1 },
        (_, idx) => ({
          task_id: null,
          attention: { ...attention, reason: `Overflow job row ${idx}.` },
        }),
      ),
      ...maxCovered,
      { task_id: 'bad task id', attention },
      { task_id: 'task_not_declared', attention },
    ],
  });
  assert.ok(overflowing);
  assert.equal(
    overflowing.covered_attention.length,
    MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION,
  );
  assert.equal(overflowing.covered_attention[0]?.task_id, null);
  assert.deepEqual(
    overflowing.covered_attention.slice(1).map((covered) => covered.task_id),
    taskIds,
  );
  assert.equal(
    overflowing.covered_attention.some(
      (covered) =>
        covered.task_id === 'bad task id' || covered.task_id === 'task_not_declared',
    ),
    false,
  );

  const oversizedLinks = Array.from(
    { length: MAX_WORKSPACE_RECORD_LINKS_PER_LIST + 5 },
    (_, idx) => `link_${idx}`,
  );
  const history = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_covered_max_01',
      binding_id: 'bnd_chat_alpha',
      chat_identity: sampleIdentity(),
      title: 'Covered attention maximum',
    },
    links: {
      follow_up_job_ids: oversizedLinks,
      architect_advice_ids: oversizedLinks,
      source_request_ids: oversizedLinks,
    },
    external_resolutions: [maxResolution],
  });
  assert.equal(history.external_resolutions?.length, 1);
  assert.equal(
    history.external_resolutions?.[0]?.covered_attention.length,
    MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION,
  );
  assert.equal(history.links.follow_up_job_ids.length, MAX_WORKSPACE_RECORD_LINKS_PER_LIST);
  assert.equal(history.links.architect_advice_ids.length, MAX_WORKSPACE_RECORD_LINKS_PER_LIST);
  assert.equal(history.links.source_request_ids.length, MAX_WORKSPACE_RECORD_LINKS_PER_LIST);
});

test('Messages projection accepts only safe sources, rejects transcripts/synthetic turns, and preserves ordering and completeness', () => {
  assert.equal(WORKSPACE_SAFE_MESSAGE_SOURCE_KINDS.length, 10);
  for (const allowed of WORKSPACE_SAFE_MESSAGE_SOURCE_KINDS) {
    assert.equal(validateSafeWorkspaceMessageSource(allowed), allowed);
  }

  for (const forbidden of [
    'transcript',
    'raw_transcript',
    'provider_transcript',
    'synthetic_user_message',
    'user_chat_turn',
    'unknown',
    '',
  ]) {
    assert.throws(
      () => validateSafeWorkspaceMessageSource(forbidden),
      (err: unknown) =>
        err instanceof WorkspaceContractError && err.code === 'forbidden_message_source',
    );
    assert.throws(
      () =>
        createSafeWorkspaceMessageItem({
          source_kind: forbidden,
          source_record_id: 'rec_1',
          source_order: { recorded_at_ms: 1_000, source_seq: 1, sub_index: 0 },
          scope: { source_id: 'default' },
          raw_summary: 'Rejected',
        }),
      (err: unknown) =>
        err instanceof WorkspaceContractError && err.code === 'forbidden_message_source',
    );
  }

  const longQuestion = 'Q'.repeat(260);
  const window = projectSafeWorkspaceMessages([
    {
      source_kind: 'question_answer',
      source_record_id: 'q_01',
      source_order: { recorded_at_ms: 2_000, source_seq: 11, sub_index: 1 },
      scope: {
        source_id: 'default',
        binding_id: 'bnd_chat_alpha',
        thread_uuid: VALID_UUID_A,
        canonical_project_scope: '/home/dev/repos/alpha-app',
        job_id: 'job_01',
      },
      actor_role: 'operator',
      raw_summary: 'Approved strict schema separation.',
    },
    {
      source_kind: 'question_prompt',
      source_record_id: 'q_01',
      source_order: { recorded_at_ms: 2_000, source_seq: 11, sub_index: 0 },
      scope: {
        source_id: 'default',
        binding_id: 'bnd_chat_alpha',
        thread_uuid: VALID_UUID_A,
        canonical_project_scope: '/home/dev/repos/alpha-app',
        job_id: 'job_01',
      },
      actor_role: 'coordinator',
      raw_summary: longQuestion,
    },
    {
      source_kind: 'plan_summary',
      source_record_id: 'plan_01',
      source_order: { recorded_at_ms: null, source_seq: 1, sub_index: 0 },
      scope: {
        source_id: 'default',
        job_id: 'job_01',
      },
      actor_role: 'planner',
      raw_summary: 'Initial plan summary without chat binding.',
      incomplete_reason: 'Legacy plan record predates thread_uuid capture.',
    },
  ]);

  assert.equal(window.availability, 'available');
  assert.equal(window.requires_transcript, false);
  assert.equal(window.allows_synthetic_user_messages, false);
  assert.equal(window.items.length, 3);

  assert.equal(window.items[0]!.source_kind, 'plan_summary');
  assert.ok(window.items[0]!.completeness.missing_fields.includes('binding_id'));
  assert.ok(window.items[0]!.completeness.missing_fields.includes('recorded_at_ms'));
  assert.equal(
    window.items[0]!.completeness.incomplete_reason,
    'Legacy plan record predates thread_uuid capture.',
  );

  assert.equal(window.items[1]!.source_kind, 'question_prompt');
  assert.equal(window.items[1]!.item_id, 'wmsg:v1:7:default:15:question_prompt:4:q_01:0');
  assert.deepEqual(window.items[1]!.scope.chat_identity, sampleIdentity());
  assert.equal(window.items[1]!.completeness.summary_truncated, true);
  assert.equal(window.items[1]!.completeness.original_char_count, 260);
  assert.equal(Array.from(window.items[1]!.summary).length, MAX_OBSERVATION_SUMMARY_CHARS);

  assert.equal(window.items[2]!.source_kind, 'question_answer');
  assert.equal(window.items[2]!.item_id, 'wmsg:v1:7:default:15:question_answer:4:q_01:1');
  assert.equal(window.items[2]!.completeness.summary_truncated, false);

  // Secret marker redaction parity: `sk-` at token boundary redacts, while `task-obs-main` does not.
  const redactedMeta = sanitizeWorkspaceSummaryWithMeta('Leaked key sk-1234567890abcdef');
  assert.equal(redactedMeta.redacted, true);
  assert.equal(redactedMeta.original_char_count, null);
  assert.equal(redactedMeta.summary, '[redacted sensitive metadata]');

  const ordinaryMeta = sanitizeWorkspaceSummaryWithMeta('Completed task-obs-main cleanly');
  assert.equal(ordinaryMeta.redacted, false);
  assert.equal(ordinaryMeta.summary, 'Completed task-obs-main cleanly');
});

test('Unicode boundaries preserve valid UTF-8 and match Rust scalar and byte counts', () => {
  const exact240 = '🦀'.repeat(240);
  const exactMeta = sanitizeWorkspaceSummaryWithMeta(exact240);
  assert.equal(exactMeta.truncated, false);
  assert.equal(exactMeta.original_char_count, 240);
  assert.equal(Array.from(exactMeta.summary).length, 240);

  const over245 = `ÖÜŞİĞÇöüşığç日本${'🦀'.repeat(235)}`;
  assert.equal(Array.from(over245).length, 249);
  const overMeta = sanitizeWorkspaceSummaryWithMeta(over245);
  assert.equal(overMeta.truncated, true);
  assert.equal(overMeta.original_char_count, 249);
  assert.equal(Array.from(overMeta.summary).length, 240);
  assert.ok(overMeta.summary.endsWith('...'));

  const unicodePath = '/home/sinan/çalışma/🦀-proje';
  assert.equal(utf8ByteLength(unicodePath), 33);
  const unicodeId = validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_utf8',
    thread_uuid: VALID_UUID_A,
    canonical_project_scope: unicodePath,
  });
  const expectedUnicodeKey =
    'wchat:v1:7:default:8:bnd_utf8:36:0196ed00-0000-7000-8000-000000000042:33:/home/sinan/çalışma/🦀-proje';
  assert.equal(formatWorkspaceChatIdentityKey(unicodeId), expectedUnicodeKey);
});

test('shared frontend contract pins Messages default, Jobs/Activity separation, per-chat Archive preferences, and compact tray bounds', () => {
  // 1. Pinned chat-first tabs and surface separation.
  assert.deepEqual(WORKSPACE_CHAT_SURFACE_TABS, ['messages', 'jobs', 'activity']);
  assert.equal(DEFAULT_WORKSPACE_CHAT_TAB, 'messages');
  assert.equal(normalizeWorkspaceChatTab(undefined), 'messages');
  assert.equal(normalizeWorkspaceChatTab('invalid_tab'), 'messages');
  assert.equal(normalizeWorkspaceChatTab('jobs'), 'jobs');
  assert.equal(normalizeWorkspaceChatTab('activity'), 'activity');

  assert.equal(WORKSPACE_SURFACE_SEPARATION.default_chat_tab, 'messages');
  assert.equal(WORKSPACE_SURFACE_SEPARATION.messages_surface.requires_transcript, false);
  assert.equal(WORKSPACE_SURFACE_SEPARATION.messages_surface.allows_synthetic_user_messages, false);
  assert.equal(WORKSPACE_SURFACE_SEPARATION.messages_surface.max_items, 1_000);
  assert.equal(
    WORKSPACE_SURFACE_SEPARATION.jobs_surface.kind,
    'durable_job_task_attempt_candidate_ledger',
  );
  assert.equal(
    WORKSPACE_SURFACE_SEPARATION.activity_surface.kind,
    'paginated_request_call_delivery_audit_feed',
  );

  // Legacy CodexChatTarget and WorkspaceScopedChatTarget remain compatible in app-contract.ts.
  const legacyTarget: CodexChatTarget = {
    source_id: 'default',
    thread_uuid: VALID_UUID_A,
    job_id: 'job_01',
  };
  const scopedTarget: WorkspaceScopedChatTarget = {
    identity: sampleIdentity(),
    job_id: 'job_01',
  };
  assert.equal(OPEN_CODEX_CHAT_COMMAND, 'desktop_open_codex_chat');
  assert.equal(legacyTarget.thread_uuid, scopedTarget.identity.thread_uuid);

  // 2. Per-chat Active / Archive lifecycle preferences keyed strictly by canonical `wchat:v1:...`.
  assert.deepEqual(WORKSPACE_CHAT_LIFECYCLE_FILTERS, ['active', 'archived']);
  assert.equal(DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER, 'active');

  const backing = new Map<string, string>();
  const memoryStorage: WorkspaceChatPreferenceStorage = {
    getItem: (key) => backing.get(key) ?? null,
    setItem: (key, value) => {
      backing.set(key, value);
    },
  };

  const idA = sampleIdentity();
  const idB = validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_chat_beta',
    thread_uuid: VALID_UUID_B,
    canonical_project_scope: '/home/dev/repos/beta-app',
  });

  let prefs = readWorkspaceChatArchivePreferences(memoryStorage);
  assert.equal(resolveWorkspaceChatLifecycle(prefs, idA), 'active');

  prefs = setWorkspaceChatArchivePreference(prefs, idA, 'archived', 10_000);
  assert.equal(resolveWorkspaceChatLifecycle(prefs, idA), 'archived');
  assert.equal(resolveWorkspaceChatLifecycle(prefs, idB), 'active');
  assert.equal(writeWorkspaceChatArchivePreferences(memoryStorage, prefs), true);

  // Inject forged title/sessionId/job_id keys into storage and verify they are ignored on read.
  const rawStored = JSON.parse(
    backing.get(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY) ?? '{}',
  ) as Record<string, unknown>;
  rawStored['Alpha Chat Title'] = {
    chat_key: 'Alpha Chat Title',
    identity: idB,
    lifecycle: 'archived',
    updated_at_ms: 20_000,
  };
  rawStored['sess_shared_123'] = {
    chat_key: 'sess_shared_123',
    identity: {
      source_id: 'default',
      binding_id: 'sess_shared_123',
      thread_uuid: VALID_UUID_B,
      canonical_project_scope: '/home/dev/repos/beta-app',
    },
    lifecycle: 'archived',
    updated_at_ms: 20_000,
  };
  backing.set(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY, JSON.stringify(rawStored));

  const reloaded = readWorkspaceChatArchivePreferences(memoryStorage);
  assert.equal(resolveWorkspaceChatLifecycle(reloaded, idA), 'archived');
  assert.equal(resolveWorkspaceChatLifecycle(reloaded, idB), 'active');
  assert.equal(Object.keys(reloaded).length, 1);

  // Restoring to `active` removes the archived entry cleanly.
  const restored = setWorkspaceChatArchivePreference(reloaded, idA, 'active', 30_000);
  assert.equal(resolveWorkspaceChatLifecycle(restored, idA), 'active');
  assert.equal(Object.keys(restored).length, 0);

  // Throwing storage degrades safely without throwing.
  const brokenStorage: WorkspaceChatPreferenceStorage = {
    getItem: () => {
      throw new Error('storage denied');
    },
    setItem: () => {
      throw new Error('quota exceeded');
    },
  };
  assert.deepEqual(readWorkspaceChatArchivePreferences(brokenStorage), {});
  assert.equal(writeWorkspaceChatArchivePreferences(brokenStorage, prefs), false);

  // 3. Compact native tray content bounds (`<= 3` rows, `<= 1` terminal result row, priority order).
  assert.equal(WORKSPACE_TRAY_MAX_ROWS, 3);
  assert.equal(WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS, 1);
  assert.equal(WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT, 50);
  assert.equal(WORKSPACE_TRAY_REFRESH_INTERVAL_MS, 4_000);
  assert.equal(WORKSPACE_TRAY_MAX_TITLE_CHARS, 80);
  assert.equal(WORKSPACE_TRAY_MAX_DETAIL_CHARS, 160);
  assert.equal(WORKSPACE_TRAY_MAX_ERROR_CHARS, 200);
  assert.deepEqual(WORKSPACE_TRAY_ACTIONS, ['show_main', 'hide', 'quit']);
  assert.deepEqual(WORKSPACE_MAIN_CLOSE_BEHAVIORS, ['hide_to_tray', 'quit']);
  assert.deepEqual(WORKSPACE_TRAY_CONTENT_BOUNDS.priority_order, [
    'needs_human',
    'working',
    'result',
  ]);

  const inputTrayRows: WorkspaceCompactTrayRow[] = [
    {
      id: 'res_older',
      kind: 'result',
      title: 'Older Result',
      detail: 'Completed job 1',
      updated_at_ms: 1_000,
      chat_identity: idA,
      job_id: 'job_01',
    },
    {
      id: 'res_newer',
      kind: 'result',
      title: 'T'.repeat(100),
      detail: 'D'.repeat(200),
      updated_at_ms: 5_000,
      chat_identity: idA,
      job_id: 'job_02',
    },
    {
      id: 'work_01',
      kind: 'working',
      title: 'Active Worker',
      detail: 'Running verification',
      updated_at_ms: 3_000,
      chat_identity: idB,
      job_id: 'job_03',
    },
    {
      id: 'attn_01',
      kind: 'needs_human',
      title: 'Approval Required',
      detail: 'Awaiting plan approval',
      updated_at_ms: 2_000,
      chat_identity: idA,
      job_id: 'job_04',
    },
  ];

  const boundedTray = boundWorkspaceCompactTrayRows(inputTrayRows);
  assert.equal(boundedTray.length, 3);
  assert.deepEqual(
    boundedTray.map((r) => r.id),
    ['attn_01', 'work_01', 'res_newer'],
  );
  assert.equal(boundedTray.filter((r) => r.kind === 'result').length, 1);
  assert.equal(Array.from(boundedTray[2]!.title).length, WORKSPACE_TRAY_MAX_TITLE_CHARS);
  assert.equal(Array.from(boundedTray[2]!.detail).length, WORKSPACE_TRAY_MAX_DETAIL_CHARS);
});

test('numeric bounds enforce existing and new sidecar, Messages, and job history ledger capacities', () => {
  assert.equal(DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT, 50);
  assert.equal(MAX_OBSERVATION_REQUEST_PAGE_LIMIT, 200);
  assert.equal(MAX_OBSERVATION_JOBS, 500);
  assert.equal(MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW, 1_000);
  assert.equal(MAX_OBSERVATION_SUMMARY_CHARS, 240);
  assert.equal(MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES, 65_536);
  assert.equal(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE, 128);
  assert.equal(MAX_WORKSPACE_MESSAGES_WINDOW, 1_000);
  assert.equal(MAX_WORKSPACE_JOBS_PER_BINDING, 64);
  assert.equal(MAX_WORKSPACE_TASKS_PER_JOB, 64);
  assert.equal(MAX_WORKSPACE_ATTEMPTS_PER_TASK, 16);
  assert.equal(MAX_WORKSPACE_CANDIDATES_PER_TASK, 16);
  assert.equal(MAX_WORKSPACE_RUNS_PER_JOB, 256);
  assert.equal(MAX_WORKSPACE_OUTBOX_PER_JOB, 256);
  assert.equal(MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB, 32);

  // 1. 64 KiB native sample sidecar cap.
  const validSample = sampleNativeLive(
    sampleIdentity(),
    'epoch_sidecar',
    sampleProcessInstance(100, 200),
    1,
    5_000,
  );
  const sidecarBytes = serializeWorkspaceNativeSampleSidecar(validSample);
  assert.ok(sidecarBytes.byteLength <= MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES);
  const parsedSidecar = parseWorkspaceNativeSampleSidecar(sidecarBytes);
  assert.equal(parsedSidecar.sequence, 1);

  const oversized = new Uint8Array(MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES + 1).fill(0x20);
  assert.throws(
    () => parseWorkspaceNativeSampleSidecar(oversized),
    (err: unknown) => err instanceof WorkspaceContractError && err.code === 'sidecar_too_large',
  );

  // 2. Max 128 native samples per source.
  const manySamples: WorkspaceNativeSample[] = [];
  for (let idx = 0; idx < 130; idx += 1) {
    const hex = (idx + 1).toString(16).padStart(12, '0');
    const id = validateWorkspaceChatIdentity({
      source_id: 'default',
      binding_id: `bnd_${idx}`,
      thread_uuid: `0196ed00-0000-7000-8000-${hex}`,
      canonical_project_scope: '/home/dev/repos/alpha-app',
    });
    manySamples.push(sampleNativeLive(id, 'epoch_cap', null, idx + 1, 10_000 + idx));
  }
  manySamples.push(
    sampleNativeLive(
      validateWorkspaceChatIdentity({
        source_id: 'ctx_secondary',
        binding_id: 'bnd_sec_1',
        thread_uuid: VALID_UUID_B,
        canonical_project_scope: '/home/dev/repos/beta-app',
      }),
      'epoch_cap',
      null,
      1,
      20_000,
    ),
  );

  const boundedSamples = boundWorkspaceNativeSamplesPerSource(manySamples);
  assert.equal(boundedSamples.samples.length, 129);
  assert.equal(boundedSamples.omittedCount, 2);

  // 3. Max 1,000 items in Messages window.
  const msgInputs = Array.from({ length: 1_005 }, (_, seq) => ({
    source_kind: 'journal_summary',
    source_record_id: `obsrec_${seq}`,
    source_order: {
      recorded_at_ms: 1_000 + seq,
      source_seq: seq,
      sub_index: 0,
    },
    scope: {
      source_id: 'default',
    },
    raw_summary: `Journal entry ${seq}`,
  }));
  const boundedMsgs = projectSafeWorkspaceMessages(msgInputs);
  assert.equal(boundedMsgs.items.length, 1_000);
  assert.equal(boundedMsgs.omitted_count, 5);
  assert.equal(boundedMsgs.has_more, true);
  assert.equal(boundedMsgs.truncated, true);

  // 4. Job history ledger capacities: 64 tasks/job, 16 attempts/task, 16 candidates/task,
  // 256 runs/job, 256 outbox/job, 32 combined candidates/job, and 64 jobs/binding.
  const oversizedHistory = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_big',
      binding_id: 'bnd_cap',
      title: 'Oversized',
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
    },
    tasks: Array.from({ length: 70 }, (_, tIdx) => ({
      task_id: `task_${tIdx}`,
      title: 'T',
      status: 'in_progress',
      attempts: Array.from({ length: 20 }, (__, a) => ({
        attempt_no: a + 1,
        status: 'completed',
      })),
      candidates: Array.from({ length: 19 }, (__, c) => ({
        candidate_id: `cand_${tIdx}_${c}`,
        attempt_no: 1,
        review_status: 'pending',
      })),
    })),
    runs: Array.from({ length: 260 }, (_, r) => ({
      run_id: `run_${r}`,
      binding_id: 'bnd_cap',
      status: 'completed',
    })),
    outbox: Array.from({ length: 262 }, (_, o) => ({
      outbox_id: `out_${o}`,
      kind: 'worker_dispatch',
      status: 'reconciled',
    })),
    combined_candidates: Array.from({ length: 35 }, (_, cc) => ({
      combined_candidate_id: `cc_${cc}`,
      status: 'pending',
    })),
  });

  assert.equal(oversizedHistory.tasks.length, 64);
  assert.equal(oversizedHistory.tasks[0]!.attempts.length, 16);
  assert.equal(oversizedHistory.tasks[0]!.attempts[0]!.attempt_no, 5);
  assert.equal(oversizedHistory.tasks[0]!.candidates.length, 16);
  assert.equal(oversizedHistory.runs.length, 256);
  assert.equal(oversizedHistory.outbox.length, 256);
  assert.equal(oversizedHistory.combined_candidates.length, 32);
  assert.equal(oversizedHistory.truncation.tasks_omitted, 6);
  assert.equal(oversizedHistory.truncation.attempts_omitted, 64 * 4);
  assert.equal(oversizedHistory.truncation.candidates_omitted, 64 * 3);
  assert.equal(oversizedHistory.truncation.runs_omitted, 4);
  assert.equal(oversizedHistory.truncation.outbox_omitted, 6);
  assert.equal(oversizedHistory.truncation.combined_candidates_omitted, 3);

  const bindingHistories: WorkspaceJobHistory[] = Array.from({ length: 68 }, (_, jIdx) =>
    normalizeWorkspaceJobHistory({
      job: {
        source_id: 'default',
        job_id: `job_bind_${jIdx}`,
        binding_id: 'bnd_same',
        title: 'Bound Job',
        state: 'completed',
        approval_status: 'approved',
        execution_status: 'completed',
        updated_at_ms: 1_000 + jIdx,
      },
    }),
  );
  const boundedJobs = boundWorkspaceJobHistories(bindingHistories);
  assert.equal(boundedJobs.histories.length, 64);
  assert.equal(boundedJobs.omittedCount, 4);
  assert.equal(boundedJobs.histories[0]!.job.job_id, 'job_bind_67');

  // 5. Legacy ObservationSnapshot caps inside WorkspaceSnapshot: 200 requests/page and 500 jobs.
  const oversizedLegacyObs: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 99_000,
    sources: [],
    jobs: Array.from({ length: 505 }, (_, idx) => ({
      source_id: 'default',
      id: `job_${idx}`,
      project_label: 'P',
      project_path: null,
      title: 'J',
      thread_uuid: null,
      thread_label: null,
      binding_id: null,
      plan_id: null,
      plan_revision: null,
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
      review_state: null,
      updated_at_ms: 1_000,
      active_call_count: 0,
      tasks: [],
      summary: '',
    })),
    requests: Array.from({ length: 205 }, (_, idx) => ({
      source_id: 'default',
      id: `obs_${idx}`,
      job_id: null,
      parent_request_id: null,
      title: 'workflow status',
      operation: 'mcp.status',
      project_label: 'P',
      binding_id: null,
      thread_uuid: null,
      thread_label: null,
      state: 'completed',
      started_at_ms: 1_000,
      finished_at_ms: 1_001,
      updated_at_ms: 1_001,
      duration_ms: 1,
      actor: {
        label: null,
        role: null,
        source: 'paired_mcp',
        model: null,
        effort: null,
        evidence: 'unknown',
      },
      executor: null,
      usage: null,
      delivery: [],
      children: [],
      children_count: 0,
      summary: 'ok',
    })),
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };

  const cappedWs = normalizeWorkspaceSnapshot({
    generated_at_ms: 99_000,
    observation: oversizedLegacyObs,
  });

  assert.equal(cappedWs.observation.requests.length, 200);
  assert.equal(cappedWs.observation.has_more, true);
  assert.equal(cappedWs.bounds.requests_capped, true);
  assert.equal(cappedWs.observation.jobs.length, 500);
  assert.equal(cappedWs.observation.jobs_has_more, true);
  assert.equal(cappedWs.bounds.jobs_capped, true);
});

test('legacy observation upgrade keeps unknown, completed, and failed out of attention_count while preserving needs_user', () => {
  const cases = [
    {
      state: 'unknown' as const,
      approval_status: 'unknown',
      execution_status: 'unknown',
      expectedActive: 0,
      expectedAttention: 0,
    },
    {
      state: 'completed' as const,
      approval_status: 'approved',
      execution_status: 'completed',
      expectedActive: 0,
      expectedAttention: 0,
    },
    {
      state: 'failed' as const,
      approval_status: 'rejected',
      execution_status: 'failed',
      expectedActive: 0,
      expectedAttention: 0,
    },
    {
      state: 'needs_user' as const,
      approval_status: 'pending',
      execution_status: 'blocked',
      expectedActive: 1,
      expectedAttention: 1,
    },
  ];

  for (const item of cases) {
    const syntheticLegacy: ObservationSnapshot = {
      schema_version: 1,
      generated_at_ms: 1_700_000_020_000,
      sources: [
        {
          id: 'default',
          label: 'alpha-app',
          project_path: '/home/dev/repos/alpha-app',
          availability: 'available',
          message: null,
        },
      ],
      jobs: [
        {
          source_id: 'default',
          id: `job_legacy_${item.state}`,
          project_label: 'alpha-app',
          project_path: '/home/dev/repos/alpha-app',
          title: `Legacy ${item.state} job`,
          thread_uuid: VALID_UUID_A,
          thread_label: 'Alpha Chat',
          binding_id: 'bnd_chat_alpha',
          plan_id: null,
          plan_revision: null,
          state: item.state,
          approval_status: item.approval_status,
          execution_status: item.execution_status,
          review_state: null,
          updated_at_ms: 1_700_000_019_500,
          active_call_count: 0,
          tasks: [],
          summary: `Synthetic legacy ${item.state} state`,
        },
      ],
      requests: [],
      next_cursor: null,
      has_more: false,
      jobs_has_more: false,
      warnings: [],
    };

    const upgraded = upgradeLegacyObservationSnapshot(syntheticLegacy);
    const parsed = parseWorkspaceSnapshot(JSON.stringify(syntheticLegacy));
    assert.deepEqual(parsed, upgraded);

    assert.equal(upgraded.chats.length, 1);
    assert.equal(upgraded.chats[0]!.active_job_count, item.expectedActive);
    assert.equal(upgraded.chats[0]!.attention_count, item.expectedAttention);

    assert.equal(upgraded.job_histories.length, 1);
    assert.equal(upgraded.job_histories[0]!.job.state, item.state);
    assert.equal(upgraded.job_histories[0]!.current_attention, null);
    assert.equal(upgraded.job_histories[0]!.historical_failure, null);
  }
});

test('chat_work worker presence compares identity, epoch, and sequence/time before liveness in either order', () => {
  const identity = sampleIdentity();
  const itemId = 'blg_presence_order';
  const execRef = 'wbe_presence_order_1';
  const makeDoingItem = (
    bindingId: string,
    id: string,
    executionRef: string,
  ) => ({
    item_id: id,
    binding_id: bindingId,
    revision: 3,
    title: 'Execute scoped worker slice',
    acceptance_condition: 'Live presence reflects newest valid evidence.',
    delivery_description: 'Deliver verified worker evidence selection.',
    kind: 'worker' as const,
    status: 'doing' as const,
    blocked_reason: null,
    current_execution_ref: executionRef,
    completion_note: null,
    completion_evidence: null,
    completed_at_ms: null,
    created_at_ms: 1_700_000_000_100,
    updated_at_ms: 1_700_000_000_900,
  });
  const makePresenceRow = (overrides: Record<string, unknown> = {}) => ({
    item_id: itemId,
    execution_ref: execRef,
    approval_id: 'apr_live_evidence',
    task_id: 'tsk_live_evidence',
    run_id: 'run_live_evidence',
    worker_id: 'wrk_live_evidence',
    claim_id: 'wcl_live_evidence',
    host_instance_id: 'hst_live_evidence_1',
    invocation_id: '0196ed00-0000-7000-8000-000000000601',
    sequence: 4,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_000,
    sample_age_ms: 100,
    max_sample_age_ms: 5_000,
    expires_at_ms: 15_000,
    ...overrides,
  });
  const item = makeDoingItem(identity.binding_id, itemId, execRef);
  const normalizePresence = (rows: Array<Record<string, unknown>>, items = [item]) => {
    const work = normalizeWorkspaceChatWork({
      identity,
      availability: 'available',
      items,
      events: [],
      worker_presence: rows,
      bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
    });
    assert.ok(work);
    return work;
  };

  const liveSeq4 = makePresenceRow();
  const nonLiveSeq5 = makePresenceRow({
    sequence: 5,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
    expires_at_ms: 15_200,
  });
  const nonLiveSeq5SameTs = makePresenceRow({
    sequence: 5,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_000,
    expires_at_ms: 15_000,
  });

  // 1. Newest explicit non_live (sequence 5) beats still-fresh older live (sequence 4)
  // in both input orders and even when sampled_at_ms ties within the same epoch.
  for (const rows of [
    [liveSeq4, nonLiveSeq5],
    [nonLiveSeq5, liveSeq4],
    [liveSeq4, nonLiveSeq5SameTs],
    [nonLiveSeq5SameTs, liveSeq4],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence.length, 1);
    assert.equal(work.worker_presence[0]!.availability, 'non_live');
    assert.equal(work.worker_presence[0]!.sequence, 5);
    assert.equal(work.worker_presence[0]!.live, false);
    assert.equal(work.bounds.presence_omitted, 1);
  }

  // 2. Inverse ordering: older non_live or unavailable (seq 4) loses to newer live (seq 5).
  const nonLiveSeq4 = makePresenceRow({
    sequence: 4,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_000,
  });
  const unavailableSeq4 = makePresenceRow({
    sequence: 4,
    availability: 'unavailable',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_000,
  });
  const liveSeq5 = makePresenceRow({
    sequence: 5,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_200,
    expires_at_ms: 15_200,
  });
  for (const rows of [
    [nonLiveSeq4, liveSeq5],
    [liveSeq5, nonLiveSeq4],
    [unavailableSeq4, liveSeq5],
    [liveSeq5, unavailableSeq4],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence.length, 1);
    assert.equal(work.worker_presence[0]!.availability, 'live');
    assert.equal(work.worker_presence[0]!.sequence, 5);
    assert.equal(work.worker_presence[0]!.live, true);
  }

  // 3. Newer unavailable observation (seq 5) clears older live (seq 4) without asserting live.
  const unavailableSeq5 = makePresenceRow({
    sequence: 5,
    availability: 'unavailable',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
    expires_at_ms: 15_200,
  });
  for (const rows of [
    [liveSeq4, unavailableSeq5],
    [unavailableSeq5, liveSeq4],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence.length, 1);
    assert.equal(work.worker_presence[0]!.availability, 'unavailable');
    assert.equal(work.worker_presence[0]!.sequence, 5);
    assert.equal(work.worker_presence[0]!.live, false);
  }

  // 4. Deterministic exact ordering ties (same epoch, seq 4, sampled_at_ms 10_000).
  for (const rows of [
    [liveSeq4, nonLiveSeq4],
    [nonLiveSeq4, liveSeq4],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence[0]!.availability, 'non_live');
    assert.equal(work.worker_presence[0]!.live, false);
  }
  const staleTie = makePresenceRow({
    freshness: 'stale',
    live: true,
    sampled_at_ms: 10_000,
  });
  for (const rows of [
    [staleTie, liveSeq4],
    [liveSeq4, staleTie],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence[0]!.freshness, 'fresh');
    assert.equal(work.worker_presence[0]!.live, true);
  }

  // 5. Restarted epoch with reset sequence=1 uses wall-clock sampled_at_ms across epochs.
  const restartedStop = makePresenceRow({
    host_instance_id: 'hst_live_evidence_2',
    invocation_id: '0196ed00-0000-7000-8000-000000000602',
    sequence: 1,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_300,
    expires_at_ms: 15_300,
  });
  const olderEpochHighSeq = makePresenceRow({
    invocation_id: '0196ed00-0000-7000-8000-000000000600',
    sequence: 99,
    sampled_at_ms: 9_000,
    expires_at_ms: 14_000,
  });
  for (const rows of [
    [liveSeq4, olderEpochHighSeq, restartedStop],
    [restartedStop, olderEpochHighSeq, liveSeq4],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence.length, 1);
    assert.equal(work.worker_presence[0]!.availability, 'non_live');
    assert.equal(work.worker_presence[0]!.sequence, 1);
    assert.equal(work.worker_presence[0]!.host_instance_id, 'hst_live_evidence_2');
    assert.equal(work.worker_presence[0]!.live, false);
  }

  const restartedLive = makePresenceRow({
    host_instance_id: 'hst_live_evidence_2',
    invocation_id: '0196ed00-0000-7000-8000-000000000602',
    sequence: 1,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_400,
    expires_at_ms: 15_400,
  });
  for (const rows of [
    [nonLiveSeq5, restartedLive],
    [restartedLive, nonLiveSeq5],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence.length, 1);
    assert.equal(work.worker_presence[0]!.live, true);
    assert.equal(work.worker_presence[0]!.sequence, 1);
  }

  const untimestampedRestarted = makePresenceRow({
    ...restartedStop,
    sampled_at_ms: null,
    sample_age_ms: null,
    expires_at_ms: null,
  });
  for (const rows of [
    [liveSeq4, untimestampedRestarted],
    [untimestampedRestarted, liveSeq4],
  ]) {
    const work = normalizePresence(rows);
    assert.equal(work.worker_presence.length, 1);
    assert.equal(work.worker_presence[0]!.live, true);
    assert.equal(work.worker_presence[0]!.sequence, 4);
  }

  // 6. Separate attempts stay isolated, and malformed rows never clear valid live evidence.
  const itemB = makeDoingItem(identity.binding_id, 'blg_presence_order_b', 'wbe_presence_order_b');
  const isolated = normalizePresence(
    [
      liveSeq4,
      makePresenceRow({ ...nonLiveSeq5, run_id: 'run_live_evidence_other' }),
      makePresenceRow({
        item_id: itemB.item_id,
        execution_ref: itemB.current_execution_ref,
        sequence: 2,
      }),
      makePresenceRow({ ...nonLiveSeq5, approval_id: '' }),
      makePresenceRow({ ...nonLiveSeq5, availability: 'bogus_state' }),
      makePresenceRow({ ...nonLiveSeq5, freshness: 'bogus_freshness' }),
      makePresenceRow({ ...nonLiveSeq5, sequence: -1 }),
      makePresenceRow({ ...nonLiveSeq5, sampled_at_ms: 0 }),
      makePresenceRow({ ...nonLiveSeq5, max_sample_age_ms: null }),
      makePresenceRow({ ...nonLiveSeq5, live: 'false' }),
    ],
    [item, itemB],
  );
  assert.equal(isolated.worker_presence.length, 3);
  assert.equal(isolated.bounds.presence_omitted, 7);
  assert.ok(
    isolated.worker_presence.some(
      (p) => p.item_id === itemId && p.run_id === 'run_live_evidence' && p.live === true,
    ),
  );
  assert.ok(
    isolated.worker_presence.some(
      (p) => p.item_id === itemId && p.run_id === 'run_live_evidence_other' && p.live === false,
    ),
  );
  assert.ok(
    isolated.worker_presence.some((p) => p.item_id === itemB.item_id && p.live === true),
  );

  // 7. Shared JSON fixture mirrored 1:1 with `crates/threadlane-core/tests/workspace_observation_contract.rs`.
  const sharedPresenceFixture = `{
    "schema_version": 1,
    "generated_at_ms": 1700000010500,
    "chat_work": [
      {
        "identity": {
          "source_id": "default",
          "binding_id": "bnd_chat_alpha",
          "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
          "canonical_project_scope": "/home/dev/repos/alpha-app"
        },
        "availability": "available",
        "items": [
          {
            "item_id": "blg_shared_stop",
            "binding_id": "bnd_chat_alpha",
            "revision": 3,
            "title": "Shared fixture stop",
            "acceptance_condition": "Newest explicit non_live wins.",
            "delivery_description": "Verify Rust/TS presence parity.",
            "kind": "worker",
            "status": "doing",
            "blocked_reason": null,
            "current_execution_ref": "wbe_shared_stop",
            "completion_note": null,
            "completion_evidence": null,
            "completed_at_ms": null,
            "created_at_ms": 1700000000100,
            "updated_at_ms": 1700000000900
          },
          {
            "item_id": "blg_shared_restarted",
            "binding_id": "bnd_chat_alpha",
            "revision": 3,
            "title": "Shared fixture restarted epoch",
            "acceptance_condition": "Restarted epoch reset sequence wins.",
            "delivery_description": "Verify epoch reconciliation parity.",
            "kind": "worker",
            "status": "doing",
            "blocked_reason": null,
            "current_execution_ref": "wbe_shared_restarted",
            "completion_note": null,
            "completion_evidence": null,
            "completed_at_ms": null,
            "created_at_ms": 1700000000200,
            "updated_at_ms": 1700000000950
          }
        ],
        "events": [],
        "worker_presence": [
          {
            "item_id": "blg_shared_stop",
            "execution_ref": "wbe_shared_stop",
            "approval_id": "apr_shared_1",
            "task_id": "tsk_shared_1",
            "run_id": "run_shared_1",
            "worker_id": "wrk_shared_1",
            "claim_id": "wcl_shared_1",
            "host_instance_id": "hst_shared_1",
            "invocation_id": "0196ed00-0000-7000-8000-000000000701",
            "sequence": 4,
            "availability": "live",
            "freshness": "fresh",
            "live": true,
            "sampled_at_ms": 1700000010000,
            "sample_age_ms": 100,
            "max_sample_age_ms": 5000,
            "expires_at_ms": 1700000015000
          },
          {
            "item_id": "blg_shared_restarted",
            "execution_ref": "wbe_shared_restarted",
            "approval_id": "apr_shared_2",
            "task_id": "tsk_shared_2",
            "run_id": "run_shared_2",
            "worker_id": "wrk_shared_2",
            "claim_id": "wcl_shared_2",
            "host_instance_id": "hst_shared_1",
            "invocation_id": "0196ed00-0000-7000-8000-000000000702",
            "sequence": 4,
            "availability": "non_live",
            "freshness": "not_applicable",
            "live": false,
            "sampled_at_ms": 1700000010000,
            "sample_age_ms": 100,
            "max_sample_age_ms": 5000,
            "expires_at_ms": 1700000015000
          }
        ],
        "bounds": { "items_omitted": 0, "events_omitted": 0, "presence_omitted": 0 }
      },
      {
        "identity": {
          "source_id": "default",
          "binding_id": "bnd_chat_alpha",
          "thread_uuid": "0196ed00-0000-7000-8000-000000000042",
          "canonical_project_scope": "/home/dev/repos/alpha-app"
        },
        "availability": "available",
        "items": [],
        "events": [],
        "worker_presence": [
          {
            "item_id": "blg_shared_stop",
            "execution_ref": "wbe_shared_stop",
            "approval_id": "apr_shared_1",
            "task_id": "tsk_shared_1",
            "run_id": "run_shared_1",
            "worker_id": "wrk_shared_1",
            "claim_id": "wcl_shared_1",
            "host_instance_id": "hst_shared_1",
            "invocation_id": "0196ed00-0000-7000-8000-000000000701",
            "sequence": 5,
            "availability": "non_live",
            "freshness": "not_applicable",
            "live": false,
            "sampled_at_ms": 1700000010200,
            "sample_age_ms": 50,
            "max_sample_age_ms": 5000,
            "expires_at_ms": 1700000015200
          },
          {
            "item_id": "blg_shared_restarted",
            "execution_ref": "wbe_shared_restarted",
            "approval_id": "apr_shared_2",
            "task_id": "tsk_shared_2",
            "run_id": "run_shared_2",
            "worker_id": "wrk_shared_2",
            "claim_id": "wcl_shared_2",
            "host_instance_id": "hst_shared_2",
            "invocation_id": "0196ed00-0000-7000-8000-000000000703",
            "sequence": 1,
            "availability": "live",
            "freshness": "fresh",
            "live": true,
            "sampled_at_ms": 1700000010300,
            "sample_age_ms": 50,
            "max_sample_age_ms": 5000,
            "expires_at_ms": 1700000015300
          }
        ],
        "bounds": { "items_omitted": 0, "events_omitted": 0, "presence_omitted": 0 }
      }
    ]
  }`;

  const parsedShared = parseWorkspaceSnapshot(sharedPresenceFixture);
  assert.equal(parsedShared.chat_work.length, 1);
  const sharedPresence = parsedShared.chat_work[0]!.worker_presence;
  assert.equal(sharedPresence.length, 2);
  assert.equal(sharedPresence[0]!.item_id, 'blg_shared_restarted');
  assert.equal(sharedPresence[0]!.live, true);
  assert.equal(sharedPresence[0]!.sequence, 1);
  assert.equal(sharedPresence[1]!.item_id, 'blg_shared_stop');
  assert.equal(sharedPresence[1]!.live, false);
  assert.equal(sharedPresence[1]!.availability, 'non_live');
  assert.equal(sharedPresence[1]!.sequence, 5);
  assert.equal(parsedShared.chat_work[0]!.bounds.presence_omitted, 2);
});
