import assert from 'node:assert/strict';
import test from 'node:test';

import {
  WORKSPACE_TRAY_MAX_DETAIL_CHARS,
  WORKSPACE_TRAY_MAX_ERROR_CHARS,
  WORKSPACE_TRAY_MAX_ROWS,
  WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS,
  WORKSPACE_TRAY_MAX_TITLE_CHARS,
  WORKSPACE_TRAY_REFRESH_INTERVAL_MS,
  WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT,
  type CodexChatTarget,
} from './app-contract.ts';
import type {
  ObservationCall,
  ObservationExecutor,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSnapshotRequest,
  ObservationSource,
  WorkspaceChatIdentity,
  WorkspaceJobHistory,
  WorkspaceJobSummary,
  WorkspaceNativeSample,
  WorkspaceRunRecord,
  WorkspaceSnapshot,
  WorkspaceTaskHistory,
} from './observation/types.ts';
import {
  applyTrayPreferencesLoadError,
  applyTrayPreferencesLoadSuccess,
  applyTrayPreferencesSaveError,
  applyTrayPreferencesSaveResult,
  createInitialTrayPreferencesState,
  normalizeTrayErrorMessage,
  projectTrayModel,
  TRAY_MAX_ROWS,
  TRAY_MAX_TERMINAL_RESULT_ROWS,
  TRAY_REFRESH_INTERVAL_MS,
  TRAY_SNAPSHOT_PAGE_LIMIT,
  TrayObservationCoordinator,
  validateTrayCodexChatTarget,
} from './tray-model.ts';
import {
  createUnavailableConfiguredSelection,
  createVerifiedConfiguredSelection,
  DEFAULT_WORKSPACE_SAMPLING_POLICY,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  normalizeWorkspaceSnapshot,
  regenerateWorkspaceSnapshotAt,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
} from './workspace-contract.ts';

const NOW_MS = Date.parse('2026-11-11T15:00:00.000Z');
const THREAD_UUID_1 = '11111111-2222-4333-8444-555555555555';
const THREAD_UUID_2 = '22222222-3333-4444-8555-666666666666';
const THREAD_UUID_3 = '33333333-4444-4555-8666-777777777777';

function makeIdentity(
  overrides: Partial<WorkspaceChatIdentity> = {}
): WorkspaceChatIdentity {
  return {
    source_id: 'src-main',
    binding_id: 'bind-alpha',
    thread_uuid: THREAD_UUID_1,
    canonical_project_scope: '/work/threadlane',
    ...overrides,
  };
}

function makeSource(
  overrides: Partial<ObservationSource> = {}
): ObservationSource {
  return {
    id: 'src-main',
    label: 'Threadlane',
    project_path: '/work/threadlane',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeExecutor(
  overrides: Partial<ObservationExecutor> = {}
): ObservationExecutor {
  return {
    tool: 'agy',
    provider: 'antigravity',
    role: 'coder',
    requested_model: 'gemini-4-argon-high',
    observed_model: 'gemini-4-argon-high',
    requested_effort: 'high',
    observed_effort: 'high',
    ...overrides,
  };
}

function makeCall(overrides: Partial<ObservationCall> = {}): ObservationCall {
  return {
    id: 'call-1',
    title: 'Implement tray projection',
    state: 'running',
    executor: makeExecutor(),
    started_at_ms: NOW_MS - 12_000,
    finished_at_ms: null,
    duration_ms: null,
    usage: null,
    summary: 'Updating compact tray model.',
    ...overrides,
  };
}

function makeRequest(
  overrides: Partial<ObservationRequest> = {}
): ObservationRequest {
  return {
    source_id: 'src-main',
    id: 'req-1',
    job_id: null,
    parent_request_id: null,
    title: 'Worker dispatch',
    operation: 'flow.handoff',
    project_label: 'Threadlane',
    binding_id: 'bind-alpha',
    thread_uuid: THREAD_UUID_1,
    thread_label: 'Tray UI',
    state: 'running',
    started_at_ms: NOW_MS - 15_000,
    finished_at_ms: null,
    updated_at_ms: NOW_MS - 4_000,
    duration_ms: null,
    actor: {
      label: 'Operator',
      role: 'operator',
      source: 'codex',
      model: null,
      effort: null,
      evidence: 'observed',
    },
    executor: makeExecutor(),
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: 'Running worker task.',
    ...overrides,
  };
}

function hasUnpairedSurrogate(value: string): boolean {
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        return true;
      }
      i += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return true;
    }
  }
  return false;
}

function makeNativeSample(
  overrides: Partial<WorkspaceNativeSample> = {}
): WorkspaceNativeSample {
  return {
    chat_identity: makeIdentity(),
    producer_epoch: 'epoch-1',
    backend_process_instance: {
      pid: 4242,
      boot_id: 'boot-linux-1',
      start_ticks: 987654,
    },
    sequence: 1,
    observed_at_ms: NOW_MS - 2_000,
    last_success_at_ms: NOW_MS - 2_000,
    availability: 'available',
    activity: 'idle',
    configured_selection: createUnavailableConfiguredSelection(),
    capabilities: {
      exact_thread_read: 'supported',
      activity_detail: 'supported',
      configured_selection: 'supported',
      wakeup_delivery: 'supported',
      per_call_identity: 'unsupported',
    },
    connection: {
      state: 'connected',
      freshness: 'fresh',
      route: 'native_codex',
      diagnostic: null,
    },
    ...overrides,
  };
}

function makeRunRecord(
  overrides: Partial<WorkspaceRunRecord> = {}
): WorkspaceRunRecord {
  return {
    run_id: 'run-1',
    worker_id: 'worker-1',
    binding_id: 'bind-alpha',
    task_id: 'task-1',
    attempt_no: 1,
    role: 'coder',
    status: 'running',
    executor: makeExecutor(),
    started_at_ms: NOW_MS - 10_000,
    finished_at_ms: null,
    updated_at_ms: NOW_MS - 4_000,
    ...overrides,
  };
}

function makeTaskHistory(
  overrides: Partial<WorkspaceTaskHistory> = {}
): WorkspaceTaskHistory {
  return {
    task_id: 'task-1',
    title: 'Migrate tray coordinator',
    role: 'coder',
    difficulty: 'hard',
    stage: 'implementation',
    status: 'running',
    depends_on: [],
    current_attempt_no: 1,
    attempts: [
      {
        attempt_no: 1,
        status: 'running',
        run_id: 'run-1',
        worker_id: 'worker-1',
        outbox_id: null,
        executor: makeExecutor(),
        started_at_ms: NOW_MS - 10_000,
        finished_at_ms: null,
        summary: 'Implementing workspace snapshot tray projection.',
      },
    ],
    candidates: [],
    active_candidate_id: null,
    accepted_candidate_id: null,
    historical_failure: null,
    current_attention: null,
    acceptance_proof: {
      status: 'unknown',
      accepted_candidate_id: null,
      combined_candidate_id: null,
      review_verdict_id: null,
      review_receipt_id: null,
      integration_receipt_id: null,
      closure_reservation_id: null,
      closure_verdict_id: null,
      verified_at_ms: null,
    },
    ...overrides,
  };
}

function makeJobHistory(
  jobOverrides: Partial<WorkspaceJobSummary> = {},
  historyOverrides: Partial<Omit<WorkspaceJobHistory, 'job'>> = {}
): WorkspaceJobHistory {
  const identity =
    jobOverrides.chat_identity === undefined
      ? makeIdentity()
      : jobOverrides.chat_identity;
  return {
    job: {
      source_id: identity?.source_id ?? 'src-main',
      job_id: 'job-1',
      binding_id: identity?.binding_id ?? 'bind-alpha',
      chat_identity: identity,
      title: 'Tray workspace migration',
      job_revision: 1,
      state: 'running',
      phase: 'execution',
      approval_status: 'approved',
      execution_status: 'running',
      review_state: null,
      created_at_ms: NOW_MS - 30_000,
      updated_at_ms: NOW_MS - 4_000,
      summary: 'Migrating tray components to WorkspaceSnapshot.',
      ...jobOverrides,
    },
    tasks: [makeTaskHistory()],
    runs: [
      makeRunRecord({
        binding_id: identity?.binding_id ?? 'bind-alpha',
      }),
    ],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: {
      source_plan_id: 'plan-1',
      source_plan_revision: 1,
      source_approval_id: null,
      source_work_brief_id: null,
      planning_grant_id: null,
      planning_reservation_id: null,
      supersedes_job_id: null,
      superseded_by_job_id: null,
      parent_job_id: null,
      follow_up_job_ids: [],
      architect_advice_ids: [],
      source_request_ids: [],
    },
    historical_failure: null,
    current_attention: null,
    acceptance_proof: {
      status: 'unknown',
      accepted_candidate_id: null,
      combined_candidate_id: null,
      review_verdict_id: null,
      review_receipt_id: null,
      integration_receipt_id: null,
      closure_reservation_id: null,
      closure_verdict_id: null,
      verified_at_ms: null,
    },
    truncation: {
      tasks_omitted: 0,
      attempts_omitted: 0,
      candidates_omitted: 0,
      runs_omitted: 0,
      outbox_omitted: 0,
      combined_candidates_omitted: 0,
    },
    ...historyOverrides,
  };
}

function makeWorkspaceSnapshot(
  overrides: {
    generated_at_ms?: number;
    observation?: Partial<ObservationSnapshot>;
    chats?: WorkspaceSnapshot['chats'];
    native_samples?: WorkspaceNativeSample[];
    job_histories?: WorkspaceJobHistory[];
    messages?: Partial<WorkspaceSnapshot['messages']>;
  } = {}
): WorkspaceSnapshot {
  const generatedAt = overrides.generated_at_ms ?? NOW_MS;
  return normalizeWorkspaceSnapshot({
    schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
    generated_at_ms: generatedAt,
    sampling_policy: DEFAULT_WORKSPACE_SAMPLING_POLICY,
    observation: {
      schema_version: 1,
      generated_at_ms: generatedAt,
      sources: [makeSource()],
      jobs: [],
      requests: [],
      next_cursor: null,
      has_more: false,
      jobs_has_more: false,
      warnings: [],
      ...overrides.observation,
    },
    chats: overrides.chats ?? [],
    native_samples: overrides.native_samples ?? [],
    job_histories: overrides.job_histories ?? [],
    messages: {
      availability: 'available',
      items: [],
      max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
      omitted_count: 0,
      has_more: false,
      truncated: false,
      requires_transcript: false,
      allows_synthetic_user_messages: false,
      ...overrides.messages,
    },
    bounds: {
      native_samples_omitted: 0,
      job_histories_omitted: 0,
      messages_omitted: 0,
      requests_capped: false,
      jobs_capped: false,
    },
  });
}

test('reuses shared workspace tray bounds without redefining or raising core limits', () => {
  assert.equal(TRAY_MAX_ROWS, WORKSPACE_TRAY_MAX_ROWS);
  assert.equal(TRAY_MAX_ROWS, 3);
  assert.equal(
    TRAY_MAX_TERMINAL_RESULT_ROWS,
    WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS
  );
  assert.equal(TRAY_MAX_TERMINAL_RESULT_ROWS, 1);
  assert.equal(TRAY_SNAPSHOT_PAGE_LIMIT, WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT);
  assert.equal(TRAY_SNAPSHOT_PAGE_LIMIT, 50);
  assert.equal(TRAY_REFRESH_INTERVAL_MS, WORKSPACE_TRAY_REFRESH_INTERVAL_MS);
  assert.equal(TRAY_REFRESH_INTERVAL_MS, 4_000);
});

test('unavailable sources, missing snapshots, and empty error states never report healthy zero counts', () => {
  const fromLoading = projectTrayModel(null, { loading: true, nowMs: NOW_MS });
  assert.equal(fromLoading.status, 'loading');
  assert.equal(fromLoading.workingCount, null);
  assert.equal(fromLoading.attentionCount, null);

  const fromNull = projectTrayModel(null, { nowMs: NOW_MS });
  assert.equal(fromNull.status, 'unavailable');
  assert.equal(fromNull.workingCount, null);
  assert.equal(fromNull.attentionCount, null);
  assert.equal(fromNull.rows.length, 0);

  const fromEmptyError = projectTrayModel(makeWorkspaceSnapshot(), {
    nowMs: NOW_MS,
    error: 'Desktop bridge disconnected.',
  });
  assert.equal(fromEmptyError.status, 'unavailable');
  assert.equal(fromEmptyError.workingCount, null);
  assert.equal(fromEmptyError.attentionCount, null);

  const fromUnreachableSource = projectTrayModel(
    makeWorkspaceSnapshot({
      observation: {
        sources: [
          makeSource({
            availability: 'unavailable',
            message: 'State directory missing',
          }),
        ],
      },
    }),
    { nowMs: NOW_MS }
  );
  assert.equal(fromUnreachableSource.status, 'unavailable');
  assert.equal(fromUnreachableSource.workingCount, null);
  assert.equal(fromUnreachableSource.attentionCount, null);
  assert.match(
    fromUnreachableSource.unavailableReason ?? '',
    /State directory missing/
  );

  // Local MCP read calls never create active work or terminal results
  const fromMcpOnly = projectTrayModel(
    makeWorkspaceSnapshot({
      observation: {
        requests: [
          makeRequest({
            id: 'req-mcp-status',
            state: 'completed',
            executor: {
              tool: 'threadlane_mcp',
              provider: 'local',
              role: null,
              requested_model: null,
              observed_model: null,
              requested_effort: null,
              observed_effort: null,
            },
          }),
        ],
      },
    }),
    { nowMs: NOW_MS }
  );
  assert.equal(fromMcpOnly.status, 'empty');
  assert.equal(fromMcpOnly.workingCount, 0);
  assert.equal(fromMcpOnly.attentionCount, 0);
  assert.equal(fromMcpOnly.rows.length, 0);
});

test('partial-source failure preserves healthy source chats while surfacing clean source warning', () => {
  const snapshot = makeWorkspaceSnapshot({
    observation: {
      sources: [
        makeSource({
          id: 'src-main',
          label: 'Threadlane Main',
          availability: 'available',
        }),
        makeSource({
          id: 'src-broken',
          label: 'Secondary Repo',
          project_path: '/work/secondary-broken',
          availability: 'unavailable',
          message: 'Permission denied reading runtime ledger',
        }),
      ],
    },
    job_histories: [
      makeJobHistory({
        job_id: 'job-active-1',
        title: 'Healthy source worker',
        state: 'running',
        updated_at_ms: NOW_MS - 3_000,
      }),
    ],
  });

  const model = projectTrayModel(snapshot, { nowMs: NOW_MS });
  assert.equal(model.status, 'ready');
  assert.equal(model.workingCount, 1);
  assert.equal(model.attentionCount, 0);
  assert.equal(model.unavailableSourceCount, 1);
  assert.equal(model.rows.length, 1);
  assert.equal(model.rows[0]?.title, 'Healthy source worker');
  assert.match(model.sourceNotice ?? '', /Secondary Repo/);
  assert.match(model.sourceNotice ?? '', /Permission denied/);
  assert.doesNotMatch(model.sourceNotice ?? '', /src-broken/);
  assert.doesNotMatch(model.sourceNotice ?? '', /\/work\/secondary-broken/);
});

test('enforces 3-row cap, at most 1 terminal result, and needs_human -> working -> result priority', () => {
  const identity2 = makeIdentity({
    binding_id: 'bind-beta',
    thread_uuid: THREAD_UUID_2,
  });
  const snapshot = makeWorkspaceSnapshot({
    job_histories: [
      makeJobHistory(
        {
          job_id: 'job-hold',
          title: 'Held migration scope',
          state: 'needs_user',
          execution_status: 'on_hold',
          updated_at_ms: NOW_MS - 20_000,
        },
        {
          runs: [],
          tasks: [],
          current_attention: {
            kind: 'on_hold',
            reason: 'Operator placed migration on hold.',
            next_step: 'Resume or cancel in Codex.',
            blocking_hold_ids: ['hold-1'],
            blocking_outbox_ids: [],
            question_id: null,
            updated_at_ms: NOW_MS - 20_000,
          },
        }
      ),
      makeJobHistory(
        {
          job_id: 'job-approval',
          chat_identity: identity2,
          binding_id: identity2.binding_id,
          title: 'Plan awaiting operator approval',
          state: 'needs_user',
          approval_status: 'pending_approval',
          execution_status: 'queued',
          updated_at_ms: NOW_MS - 10_000,
        },
        {
          runs: [],
          tasks: [],
          current_attention: {
            kind: 'needs_approval',
            reason: 'Published plan revision requires explicit human approval.',
            next_step: 'Approve plan in Threadlane or Codex.',
            blocking_hold_ids: [],
            blocking_outbox_ids: [],
            question_id: null,
            updated_at_ms: NOW_MS - 10_000,
          },
        }
      ),
      makeJobHistory({
        job_id: 'job-work-older',
        title: 'Older running worker',
        state: 'running',
        updated_at_ms: NOW_MS - 18_000,
      }),
      makeJobHistory({
        job_id: 'job-work-newer',
        title: 'Newest running worker',
        state: 'running',
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJobHistory(
        {
          job_id: 'job-accepted',
          title: 'Verified closure package',
          state: 'completed',
          execution_status: 'completed',
          updated_at_ms: NOW_MS - 1_000,
        },
        {
          runs: [],
          tasks: [],
          acceptance_proof: {
            status: 'verified_accepted',
            accepted_candidate_id: null,
            combined_candidate_id: 'cc-1',
            review_verdict_id: 'rv-1',
            review_receipt_id: 'rr-1',
            integration_receipt_id: 'ir-1',
            closure_reservation_id: 'cr-1',
            closure_verdict_id: 'cv-1',
            verified_at_ms: NOW_MS - 1_000,
          },
        }
      ),
    ],
  });

  const model = projectTrayModel(snapshot, { nowMs: NOW_MS });
  assert.equal(model.status, 'ready');
  assert.equal(model.workingCount, 2);
  assert.equal(model.attentionCount, 2);
  assert.equal(model.rows.length, WORKSPACE_TRAY_MAX_ROWS);
  assert.deepEqual(
    model.rows.map((row) => [row.category, row.title]),
    [
      ['needs_human', 'Plan awaiting operator approval'],
      ['needs_human', 'Held migration scope'],
      ['working', 'Newest running worker'],
    ]
  );

  // When only multiple terminal results exist, at most 1 result row is emitted
  const resultsOnlySnapshot = makeWorkspaceSnapshot({
    job_histories: [
      makeJobHistory(
        {
          job_id: 'job-accepted-older',
          title: 'Older verified package',
          state: 'completed',
          updated_at_ms: NOW_MS - 60_000,
        },
        {
          runs: [],
          tasks: [],
          acceptance_proof: {
            status: 'verified_accepted',
            accepted_candidate_id: null,
            combined_candidate_id: 'cc-old',
            review_verdict_id: null,
            review_receipt_id: null,
            integration_receipt_id: null,
            closure_reservation_id: null,
            closure_verdict_id: 'cv-old',
            verified_at_ms: NOW_MS - 60_000,
          },
        }
      ),
      makeJobHistory(
        {
          job_id: 'job-failed-newer',
          title: 'Latest failed verification',
          state: 'failed',
          execution_status: 'failed',
          summary: 'Clippy lint gate failed.',
          updated_at_ms: NOW_MS - 5_000,
        },
        {
          runs: [],
          tasks: [],
        }
      ),
    ],
  });

  const resultsOnlyModel = projectTrayModel(resultsOnlySnapshot, {
    nowMs: NOW_MS,
  });
  assert.equal(resultsOnlyModel.rows.length, WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS);
  assert.equal(resultsOnlyModel.rows[0]?.category, 'result');
  assert.equal(resultsOnlyModel.rows[0]?.tone, 'danger');
  assert.equal(resultsOnlyModel.rows[0]?.statusLabel, 'Failed');
  assert.equal(resultsOnlyModel.rows[0]?.title, 'Latest failed verification');
});

test('bounds long multibyte Unicode content cleanly without surrogate splits or raw ID/path clutter', () => {
  const longMultibyteTitle =
    'Çok baytlı görev başlığı ğüşıöçĞÜŞİÖÇ 日本語のタイトル 🚀🧪👨‍👩‍👧‍👦 '.repeat(
      4
    );
  const longMultibyteSummary =
    'Ayrıntılı özet: ğüşıöçĞÜŞİÖÇ Türkçe karakterler ve 漢字テキスト ile emoji dizileri 🚀🔥🧪 korunmalı ve sınırlandırılmalı. '.repeat(
      4
    );

  const snapshot = makeWorkspaceSnapshot({
    chats: [
      {
        identity: makeIdentity(),
        thread_label: 'Sohbet Başlığı ğüşıöç',
        project_label: 'Threadlane Çekirdek',
        availability: 'available',
        activity: 'idle',
        configured_selection: createUnavailableConfiguredSelection(),
        active_job_count: 1,
        attention_count: 0,
        updated_at_ms: NOW_MS - 2_000,
      },
    ],
    job_histories: [
      makeJobHistory({
        job_id: 'job-audit-999',
        title: longMultibyteTitle,
        summary: longMultibyteSummary,
        state: 'running',
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
  });

  const model = projectTrayModel(snapshot, { nowMs: NOW_MS });
  assert.equal(model.rows.length, 1);
  const row = model.rows[0]!;

  assert.ok(Array.from(row.title).length <= WORKSPACE_TRAY_MAX_TITLE_CHARS);
  assert.ok(Array.from(row.detail).length <= WORKSPACE_TRAY_MAX_DETAIL_CHARS);
  assert.equal(hasUnpairedSurrogate(row.title), false);
  assert.equal(hasUnpairedSurrogate(row.detail), false);

  // Preserves source language in summary while keeping UI status chrome in English
  assert.match(row.detail, /Ayrıntılı özet: ğüşıöçĞÜŞİÖÇ/);
  assert.equal(row.statusLabel, 'Worker running');
  assert.equal(row.chatLabel, 'Sohbet Başlığı ğüşıöç');
  assert.equal(row.projectLabel, 'Threadlane Çekirdek');

  // Never leaks raw audit IDs or canonical filesystem paths into compact row copy
  assert.doesNotMatch(row.title, /job-audit-999|src-main|bind-alpha/);
  assert.doesNotMatch(row.detail, /job-audit-999|\/work\/threadlane/);
  assert.doesNotMatch(row.projectLabel, /\/work\/threadlane/);
  assert.doesNotMatch(row.chatLabel, new RegExp(THREAD_UUID_1));

  // Error normalization also respects WORKSPACE_TRAY_MAX_ERROR_CHARS and redacts secrets
  const sanitizedErr = normalizeTrayErrorMessage(
    `Bearer super-secret-jwt-999 ${'Çok uzun hata 🚀 '.repeat(30)}`
  );
  assert.ok(Array.from(sanitizedErr).length <= WORKSPACE_TRAY_MAX_ERROR_CHARS);
  assert.equal(hasUnpairedSurrogate(sanitizedErr), false);
  assert.doesNotMatch(sanitizedErr, /super-secret-jwt-999/);
});

test('separates Coworker native states (idle, waiting, disconnected, stale) and configured selection from Worker/job telemetry', () => {
  const idIdleWithWorker = makeIdentity({
    binding_id: 'bind-1',
    thread_uuid: THREAD_UUID_1,
  });
  const idWaitingInput = makeIdentity({
    binding_id: 'bind-2',
    thread_uuid: THREAD_UUID_2,
  });
  const idDisconnected = makeIdentity({
    binding_id: 'bind-3',
    thread_uuid: THREAD_UUID_3,
  });

  const snapshot = makeWorkspaceSnapshot({
    native_samples: [
      makeNativeSample({
        chat_identity: idIdleWithWorker,
        availability: 'available',
        activity: 'idle',
        configured_selection: createVerifiedConfiguredSelection(
          'gpt-6-astra',
          'high'
        ),
        observed_at_ms: NOW_MS - 1_500,
        last_success_at_ms: NOW_MS - 1_500,
      }),
      makeNativeSample({
        chat_identity: idWaitingInput,
        availability: 'available',
        activity: 'waiting_for_input',
        observed_at_ms: NOW_MS - 1_000,
        last_success_at_ms: NOW_MS - 1_000,
      }),
      makeNativeSample({
        chat_identity: idDisconnected,
        availability: 'disconnected',
        activity: 'unknown',
        connection: {
          state: 'disconnected',
          freshness: 'unknown',
          route: 'native_codex',
          diagnostic: 'Socket closed',
        },
        observed_at_ms: NOW_MS - 3_000,
        last_success_at_ms: NOW_MS - 9_000,
      }),
    ],
    job_histories: [
      makeJobHistory({
        job_id: 'job-worker-active',
        chat_identity: idIdleWithWorker,
        binding_id: idIdleWithWorker.binding_id,
        title: 'Background worker slice',
        state: 'running',
        summary: null,
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
  });

  const model = projectTrayModel(snapshot, { nowMs: NOW_MS });
  assert.equal(model.status, 'ready');
  assert.equal(model.attentionCount, 1);
  assert.equal(model.workingCount, 1);
  assert.equal(model.coworkerSummary?.waitingCount, 1);
  assert.equal(model.coworkerSummary?.idleCount, 1);
  assert.equal(model.coworkerSummary?.disconnectedCount, 1);
  assert.match(model.coworkerSummary?.label ?? '', /1 disconnected/);

  // Row 0: Native Coworker waiting for input -> needs_human
  assert.equal(model.rows[0]?.category, 'needs_human');
  assert.equal(model.rows[0]?.statusLabel, 'Coworker waiting for input');

  // Row 1: Running Worker while Coworker is idle -> never marked Done, configured model never leaked as runtime telemetry
  assert.equal(model.rows[1]?.category, 'working');
  assert.equal(model.rows[1]?.statusLabel, 'Worker running');
  assert.equal(model.rows[1]?.coworkerLabel, 'Coworker idle');
  assert.doesNotMatch(model.rows[1]?.detail ?? '', /gpt-6-astra/);

  // Stale Coworker sample (> 10,000 ms) withholds live waiting/working claim
  const staleSnapshot = makeWorkspaceSnapshot({
    native_samples: [
      makeNativeSample({
        chat_identity: idIdleWithWorker,
        availability: 'available',
        activity: 'waiting_for_approval',
        configured_selection: createVerifiedConfiguredSelection(
          'gpt-6-astra',
          'high'
        ),
        observed_at_ms: NOW_MS - 15_000,
        last_success_at_ms: NOW_MS - 15_000,
      }),
    ],
  });

  const staleModel = projectTrayModel(staleSnapshot, { nowMs: NOW_MS });
  assert.equal(staleModel.status, 'empty');
  assert.equal(staleModel.attentionCount, 0);
  assert.equal(staleModel.workingCount, 0);
  assert.equal(staleModel.isStale, true);
  assert.equal(staleModel.coworkerSummary?.tone, 'stale');
  assert.match(staleModel.coworkerSummary?.label ?? '', /Coworker stale \(15s\)/);
  assert.match(staleModel.emptyMessage ?? '', /withheld/);
});

test('historical failures never become current blockers, and unaccepted/unknown Worker outputs never appear as Accepted or Done', () => {
  // 1. Historical failure on attempt #1 superseded by active attempt #2
  const historicalFailureJob = makeJobHistory(
    {
      job_id: 'job-retry',
      title: 'Retried worker job',
      state: 'running',
      updated_at_ms: NOW_MS - 3_000,
    },
    {
      historical_failure: {
        failure_id: 'fail-old-1',
        stage: 'execution',
        failed_at_ms: NOW_MS - 90_000,
        task_id: 'task-1',
        attempt_no: 1,
        run_id: 'run-old-1',
        outbox_id: null,
        reason: 'Attempt 1 timed out.',
        superseded_or_resolved: true,
      },
      tasks: [
        makeTaskHistory({
          current_attempt_no: 2,
          attempts: [
            {
              attempt_no: 1,
              status: 'failed',
              run_id: 'run-old-1',
              worker_id: 'worker-1',
              outbox_id: null,
              executor: makeExecutor(),
              started_at_ms: NOW_MS - 100_000,
              finished_at_ms: NOW_MS - 90_000,
              summary: 'Timed out.',
            },
            {
              attempt_no: 2,
              status: 'running',
              run_id: 'run-2',
              worker_id: 'worker-2',
              outbox_id: null,
              executor: makeExecutor(),
              started_at_ms: NOW_MS - 5_000,
              finished_at_ms: null,
              summary: 'Running clean retry.',
            },
          ],
          historical_failure: {
            failure_id: 'fail-old-1',
            stage: 'execution',
            failed_at_ms: NOW_MS - 90_000,
            task_id: 'task-1',
            attempt_no: 1,
            run_id: 'run-old-1',
            outbox_id: null,
            reason: 'Attempt 1 timed out.',
            superseded_or_resolved: true,
          },
        }),
      ],
      runs: [
        makeRunRecord({
          run_id: 'run-2',
          task_id: 'task-1',
          attempt_no: 2,
          worker_id: 'worker-2',
          status: 'running',
          started_at_ms: NOW_MS - 5_000,
          finished_at_ms: null,
          updated_at_ms: NOW_MS - 3_000,
        }),
      ],
    }
  );

  const retryModel = projectTrayModel(
    makeWorkspaceSnapshot({ job_histories: [historicalFailureJob] }),
    { nowMs: NOW_MS }
  );
  assert.equal(retryModel.attentionCount, 0);
  assert.equal(retryModel.workingCount, 1);
  assert.equal(retryModel.rows[0]?.category, 'working');

  // 2. Worker finished with prose claiming "Accepted and Done!" and native Coworker idle,
  //    WITHOUT valid closure_verdict_id + combined_candidate_id proof
  const unacceptedSnapshot = makeWorkspaceSnapshot({
    native_samples: [
      makeNativeSample({
        availability: 'available',
        activity: 'idle',
      }),
    ],
    job_histories: [
      makeJobHistory(
        {
          job_id: 'job-unverified',
          title: 'Worker completed implementation',
          state: 'completed',
          execution_status: 'accepted',
          summary: 'Done! Accepted and validated all checks.',
          updated_at_ms: NOW_MS - 1_000,
        },
        {
          runs: [
            makeRunRecord({
              run_id: 'run-done',
              task_id: 'task-1',
              attempt_no: 1,
              worker_id: 'worker-1',
              status: 'completed',
              started_at_ms: NOW_MS - 20_000,
              finished_at_ms: NOW_MS - 1_000,
              updated_at_ms: NOW_MS - 1_000,
            }),
          ],
          tasks: [
            makeTaskHistory({
              status: 'completed',
              attempts: [
                {
                  attempt_no: 1,
                  status: 'completed',
                  run_id: 'run-done',
                  worker_id: 'worker-1',
                  outbox_id: null,
                  executor: makeExecutor(),
                  started_at_ms: NOW_MS - 20_000,
                  finished_at_ms: NOW_MS - 1_000,
                  summary: 'Done! Accepted!',
                },
              ],
            }),
          ],
          // Incomplete proof (missing closure_verdict_id) must downgrade to unknown
          acceptance_proof: {
            status: 'verified_accepted',
            accepted_candidate_id: 'cand-1',
            combined_candidate_id: 'cc-1',
            review_verdict_id: 'rv-1',
            review_receipt_id: null,
            integration_receipt_id: null,
            closure_reservation_id: null,
            closure_verdict_id: null,
            verified_at_ms: NOW_MS - 1_000,
          },
        }
      ),
    ],
  });

  const unacceptedModel = projectTrayModel(unacceptedSnapshot, {
    nowMs: NOW_MS,
  });
  assert.equal(unacceptedModel.pendingAcceptanceCount, 1);
  assert.equal(unacceptedModel.rows.length, 1);
  assert.equal(unacceptedModel.rows[0]?.tone, 'neutral');
  assert.equal(unacceptedModel.rows[0]?.statusLabel, 'Awaiting acceptance');
  assert.equal(unacceptedModel.rows[0]?.coworkerLabel, 'Coworker idle');
  assert.notEqual(unacceptedModel.rows[0]?.tone, 'success');
  assert.doesNotMatch(
    unacceptedModel.rows[0]?.statusLabel ?? '',
    /^(Done|Accepted|Verified accepted|Completed)$/i
  );

  // 3. An unaccepted job completion never displaces an older real terminal result,
  //    and an unknown worker outcome surfaces in needs_human with unknown tone
  const mixedTerminalAndUnknown = makeWorkspaceSnapshot({
    job_histories: [
      makeJobHistory(
        {
          job_id: 'job-real-accepted',
          title: 'Earlier verified milestone',
          state: 'completed',
          updated_at_ms: NOW_MS - 50_000,
        },
        {
          runs: [],
          tasks: [],
          acceptance_proof: {
            status: 'verified_accepted',
            accepted_candidate_id: null,
            combined_candidate_id: 'cc-verified',
            review_verdict_id: null,
            review_receipt_id: null,
            integration_receipt_id: null,
            closure_reservation_id: null,
            closure_verdict_id: 'cv-verified',
            verified_at_ms: NOW_MS - 50_000,
          },
        }
      ),
      makeJobHistory(
        {
          job_id: 'job-newer-unaccepted',
          title: 'Newer unaccepted worker output',
          state: 'completed',
          updated_at_ms: NOW_MS - 1_000,
        },
        {
          runs: [],
          tasks: [],
          acceptance_proof: {
            status: 'pending_review',
            accepted_candidate_id: null,
            combined_candidate_id: null,
            review_verdict_id: null,
            review_receipt_id: null,
            integration_receipt_id: null,
            closure_reservation_id: null,
            closure_verdict_id: null,
            verified_at_ms: null,
          },
        }
      ),
      makeJobHistory(
        {
          job_id: 'job-unknown-attempt',
          title: 'Interrupted worker handoff',
          state: 'ready',
          updated_at_ms: NOW_MS - 5_000,
        },
        {
          runs: [],
          tasks: [
            makeTaskHistory({
              status: 'unknown',
              current_attempt_no: 1,
              attempts: [
                {
                  attempt_no: 1,
                  status: 'unknown',
                  run_id: 'run-unk',
                  worker_id: 'worker-unk',
                  outbox_id: 'outbox-unk',
                  executor: makeExecutor(),
                  started_at_ms: NOW_MS - 8_000,
                  finished_at_ms: null,
                  summary: 'Worker process exited before receipt.',
                },
              ],
            }),
          ],
        }
      ),
    ],
  });

  const mixedModel = projectTrayModel(mixedTerminalAndUnknown, {
    nowMs: NOW_MS,
  });
  assert.equal(mixedModel.attentionCount, 1);
  assert.equal(mixedModel.rows.length, 2);
  assert.equal(mixedModel.rows[0]?.category, 'needs_human');
  assert.equal(mixedModel.rows[0]?.tone, 'unknown');
  assert.equal(mixedModel.rows[0]?.statusLabel, 'Status unknown');
  assert.equal(mixedModel.rows[1]?.category, 'result');
  assert.equal(mixedModel.rows[1]?.tone, 'success');
  assert.equal(mixedModel.rows[1]?.statusLabel, 'Verified accepted');
  assert.equal(mixedModel.rows[1]?.title, 'Earlier verified milestone');

  // 4. Standalone completed Worker call (without a verified accepted job) never creates
  //    a terminal result row, while parallel running child Worker calls count independently
  const standaloneCompletedWorker = projectTrayModel(
    makeWorkspaceSnapshot({
      observation: {
        requests: [
          makeRequest({
            id: 'req-standalone-done',
            job_id: null,
            state: 'completed',
            finished_at_ms: NOW_MS - 1_500,
            updated_at_ms: NOW_MS - 1_500,
            children_count: 1,
            children: [
              makeCall({
                id: 'call-standalone-done',
                state: 'completed',
                finished_at_ms: NOW_MS - 1_500,
                summary: 'Worker finished patch.',
              }),
            ],
          }),
        ],
      },
    }),
    { nowMs: NOW_MS }
  );
  assert.equal(standaloneCompletedWorker.status, 'empty');
  assert.equal(standaloneCompletedWorker.rows.length, 0);
  assert.equal(standaloneCompletedWorker.pendingAcceptanceCount, 1);
  assert.match(
    standaloneCompletedWorker.emptyMessage ?? '',
    /awaiting verified job acceptance/
  );

  const parallelWorkerCalls = projectTrayModel(
    makeWorkspaceSnapshot({
      observation: {
        requests: [
          makeRequest({
            id: 'req-parallel',
            job_id: null,
            state: 'running',
            children_count: 2,
            children: [
              makeCall({
                id: 'call-parallel-1',
                title: 'Coder slice A',
                state: 'running',
                started_at_ms: NOW_MS - 6_000,
              }),
              makeCall({
                id: 'call-parallel-2',
                title: 'Coder slice B',
                state: 'running',
                started_at_ms: NOW_MS - 3_000,
              }),
            ],
          }),
        ],
      },
    }),
    { nowMs: NOW_MS }
  );
  assert.equal(parallelWorkerCalls.status, 'ready');
  assert.equal(parallelWorkerCalls.workingCount, 2);
  assert.equal(parallelWorkerCalls.rows.length, 2);
  assert.equal(parallelWorkerCalls.rows[0]?.title, 'Coder slice B');
  assert.equal(parallelWorkerCalls.rows[1]?.title, 'Coder slice A');
});

test('retains producer timestamps across snapshot regeneration and refresh failure so sample age advances truthfully', async () => {
  const sampleObservedAt = NOW_MS - 2_000;
  const initialSnapshot = makeWorkspaceSnapshot({
    generated_at_ms: NOW_MS,
    native_samples: [
      makeNativeSample({
        availability: 'available',
        activity: 'working',
        observed_at_ms: sampleObservedAt,
        last_success_at_ms: sampleObservedAt,
      }),
    ],
  });

  // Regenerating snapshot at NOW + 12s never resets sample timestamps
  const regenerated = regenerateWorkspaceSnapshotAt(
    initialSnapshot,
    NOW_MS + 12_000
  );
  assert.equal(regenerated.native_samples[0]?.observed_at_ms, sampleObservedAt);
  assert.equal(
    regenerated.native_samples[0]?.last_success_at_ms,
    sampleObservedAt
  );
  const regeneratedModel = projectTrayModel(regenerated, {
    nowMs: NOW_MS + 12_000,
  });
  assert.equal(regeneratedModel.workingCount, 0);
  assert.equal(regeneratedModel.isStale, true);
  assert.match(
    regeneratedModel.coworkerSummary?.label ?? '',
    /Coworker stale \(14s\)/
  );

  // Coordinator retains last snapshot on refresh failure without resetting producer timestamps
  let attempt = 0;
  const coordinator = new TrayObservationCoordinator(async () => {
    attempt += 1;
    if (attempt === 1) {
      return initialSnapshot;
    }
    throw new Error('IPC desktop_workspace_snapshot timed out');
  });

  await coordinator.open();
  const freshModel = projectTrayModel(coordinator.getState().snapshot, {
    loading: coordinator.getState().loading,
    error: coordinator.getState().error,
    nowMs: NOW_MS,
  });
  assert.equal(freshModel.status, 'ready');
  assert.equal(freshModel.workingCount, 1);
  assert.equal(freshModel.isStale, false);

  // Second read fails 13 seconds later; retained snapshot sample age is now 15s (stale)
  await coordinator.refresh({ force: true });
  const retainedState = coordinator.getState();
  assert.notEqual(retainedState.snapshot, null);
  assert.equal(
    retainedState.snapshot?.native_samples[0]?.observed_at_ms,
    sampleObservedAt
  );
  assert.equal(
    retainedState.snapshot?.native_samples[0]?.last_success_at_ms,
    sampleObservedAt
  );
  assert.match(retainedState.error ?? '', /timed out/);

  const retainedModel = projectTrayModel(retainedState.snapshot, {
    loading: retainedState.loading,
    error: retainedState.error,
    nowMs: NOW_MS + 13_000,
  });
  assert.equal(retainedModel.isStale, true);
  assert.match(retainedModel.refreshError ?? '', /timed out/);
  assert.equal(retainedModel.workingCount, 0);
  assert.match(
    retainedModel.coworkerSummary?.label ?? '',
    /Coworker stale \(15s\)/
  );
});

test('coordinator enforces 50-request limit, single open read, coalescing, hidden-poll suppression, open/close/reopen race safety, and clean unmount', async () => {
  const recordedRequests: Array<ObservationSnapshotRequest | undefined> = [];
  const resolvers: Array<(snap: WorkspaceSnapshot) => void> = [];

  const coordinator = new TrayObservationCoordinator(
    (request) => {
      recordedRequests.push(request);
      return new Promise<WorkspaceSnapshot>((resolve) => {
        resolvers.push(resolve);
      });
    },
    undefined,
    { visible: false, focused: false }
  );

  // Hidden window does not poll and ignores background refresh calls
  assert.equal(coordinator.canPoll(), false);
  await coordinator.refresh();
  assert.equal(recordedRequests.length, 0);

  // Opening causes exactly one current fetch even when DOM focus/visibility events fire right after
  const openPromise1 = coordinator.open();
  const focusTransitionPromise = coordinator.notifyVisibilityTransition(
    true,
    true
  );
  const duplicatePollPromise = coordinator.refresh();
  assert.equal(recordedRequests.length, 1);
  assert.deepEqual(recordedRequests[0], { limit: 50 });

  // Open -> Close -> Reopen race while Request #1 is still in flight:
  // Closing invalidates Request #1; reopening queues exactly one fresh Request #2
  coordinator.close();
  assert.equal(coordinator.canPoll(), false);

  const reopenPromise = coordinator.open();
  assert.equal(coordinator.canPoll(), true);
  assert.equal(recordedRequests.length, 1);

  const staleSnapshot = makeWorkspaceSnapshot({
    job_histories: [
      makeJobHistory({ job_id: 'job-stale', title: 'Stale pre-close job' }),
    ],
  });
  resolvers[0]!(staleSnapshot);

  await Promise.resolve();
  assert.equal(coordinator.getState().snapshot, null);
  assert.equal(recordedRequests.length, 2);
  assert.deepEqual(recordedRequests[1], { limit: 50 });

  const freshSnapshot = makeWorkspaceSnapshot({
    job_histories: [
      makeJobHistory({ job_id: 'job-fresh', title: 'Fresh reopened job' }),
    ],
  });
  resolvers[1]!(freshSnapshot);
  await Promise.all([openPromise1, focusTransitionPromise, duplicatePollPromise, reopenPromise]);

  assert.equal(
    coordinator.getState().snapshot?.job_histories[0]?.job.title,
    'Fresh reopened job'
  );
  assert.equal(coordinator.getState().loading, false);

  // Unmount / dispose suppresses polling and discards any late in-flight response
  const latePromise = coordinator.refresh({ force: true });
  assert.equal(recordedRequests.length, 3);
  coordinator.dispose();
  assert.equal(coordinator.canPoll(), false);
  resolvers[2]!(staleSnapshot);
  await latePromise;
  assert.equal(
    coordinator.getState().snapshot?.job_histories[0]?.job.title,
    'Fresh reopened job'
  );
});

test('narrow CodexChatTarget navigation validates identity fields and fails explicitly on unsupported routes', async () => {
  const openedTargets: CodexChatTarget[] = [];
  let failNextOpen = false;

  const coordinator = new TrayObservationCoordinator(
    async () => makeWorkspaceSnapshot(),
    undefined,
    {
      openCodexChat: async (target) => {
        if (failNextOpen) {
          throw new Error(
            'Opening a Codex chat is not supported on this route.'
          );
        }
        openedTargets.push(target);
      },
    }
  );

  const validTarget: CodexChatTarget = {
    source_id: 'src-main',
    thread_uuid: THREAD_UUID_1,
    job_id: 'job-1',
  };
  const ok = await coordinator.openCodexChat(validTarget);
  assert.equal(ok, true);
  assert.deepEqual(openedTargets, [validTarget]);
  assert.equal(coordinator.getState().openError, null);

  // Rejects non-canonical UUIDs, URIs, job_id-as-source_id substitutes, and extra smuggled keys
  assert.equal(
    await coordinator.openCodexChat({
      source_id: 'src-main',
      thread_uuid: `codex://threads/${THREAD_UUID_1}`,
      job_id: null,
    }),
    false
  );
  assert.equal(
    await coordinator.openCodexChat({
      source_id: 'job-substitute',
      thread_uuid: THREAD_UUID_1,
      job_id: null,
    }),
    false
  );
  assert.equal(
    validateTrayCodexChatTarget({
      source_id: 'src-main',
      thread_uuid: THREAD_UUID_1,
      job_id: null,
      executable: '/bin/sh',
    }),
    null
  );
  assert.equal(openedTargets.length, 1);
  assert.match(coordinator.getState().openError ?? '', /canonical thread UUID/);

  // Explicit failure when backend rejects unsupported or unmatched route
  failNextOpen = true;
  const routeFailed = await coordinator.openCodexChat(validTarget);
  assert.equal(routeFailed, false);
  assert.match(
    coordinator.getState().openError ?? '',
    /not supported on this route/
  );
});

test('tray preferences transitions preserve confirmed value on save error and handle unavailable support', () => {
  const initial = createInitialTrayPreferencesState(true);
  assert.equal(initial.availability, 'loading');
  assert.equal(initial.closeBehavior, 'hide_to_tray');

  const loaded = applyTrayPreferencesLoadSuccess({
    close_behavior: 'hide_to_tray',
    tray_available: true,
    message: null,
  });
  assert.equal(loaded.availability, 'available');
  assert.equal(loaded.closeBehavior, 'hide_to_tray');

  const failedSave = applyTrayPreferencesSaveError(
    { ...loaded, saving: true },
    new Error('Failed to persist preference to disk')
  );
  assert.equal(failedSave.closeBehavior, 'hide_to_tray');
  assert.equal(failedSave.saving, false);
  assert.match(failedSave.error ?? '', /Failed to persist preference/);

  const savedQuit = applyTrayPreferencesSaveResult(failedSave, {
    close_behavior: 'quit',
    tray_available: true,
    message: null,
  });
  assert.equal(savedQuit.closeBehavior, 'quit');
  assert.equal(savedQuit.error, null);

  const unavailableFromCommand = applyTrayPreferencesLoadError(
    'unknown command desktop_tray_preferences'
  );
  assert.equal(unavailableFromCommand.availability, 'unavailable');
  assert.equal(unavailableFromCommand.closeBehavior, 'hide_to_tray');
  assert.equal(unavailableFromCommand.error, null);
});
