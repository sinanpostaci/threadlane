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
  MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB,
  MAX_WORKSPACE_JOBS_PER_BINDING,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE,
  MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
  MAX_WORKSPACE_OUTBOX_PER_JOB,
  MAX_WORKSPACE_RUNS_PER_JOB,
  MAX_WORKSPACE_TASKS_PER_JOB,
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
} from './workspace-contract.ts';
import type {
  ObservationProcessInstance,
  WorkspaceChatIdentity,
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
