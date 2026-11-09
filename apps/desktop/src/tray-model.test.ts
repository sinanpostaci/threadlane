import assert from 'node:assert/strict';
import test from 'node:test';

import type {
  ObservationCall,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
} from './observation/types.ts';
import {
  applyTrayPreferencesLoadError,
  applyTrayPreferencesLoadSuccess,
  applyTrayPreferencesSaveError,
  applyTrayPreferencesSaveResult,
  createInitialTrayPreferencesState,
  projectTrayModel,
  TRAY_MAX_ROWS,
  TrayObservationCoordinator,
} from './tray-model.ts';

const NOW_MS = Date.parse('2026-11-09T01:00:00.000Z');

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
    title: 'Implement tray model',
    state: 'running',
    executor: makeExecutor(),
    started_at_ms: NOW_MS - 15_000,
    finished_at_ms: null,
    duration_ms: null,
    usage: null,
    summary: 'Working on tray projection.',
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
    binding_id: 'bind-1',
    thread_uuid: 'thread-1',
    thread_label: 'Tray UI',
    state: 'running',
    started_at_ms: NOW_MS - 20_000,
    finished_at_ms: null,
    updated_at_ms: NOW_MS - 5_000,
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
    summary: 'Running task.',
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'src-main',
    id: 'job-1',
    project_label: 'Threadlane',
    project_path: '/work/threadlane',
    title: 'Tray popover implementation',
    thread_uuid: 'thread-1',
    thread_label: 'Tray UI',
    binding_id: 'bind-1',
    plan_id: 'plan-1',
    plan_revision: 1,
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS - 4_000,
    active_call_count: 1,
    tasks: [
      { id: 't-1', title: 'Build model', status: 'running', depends_on: [] },
    ],
    summary: 'Implementing tray view.',
    ...overrides,
  };
}

function makeSnapshot(
  overrides: Partial<ObservationSnapshot> = {}
): ObservationSnapshot {
  return {
    schema_version: 1,
    generated_at_ms: NOW_MS,
    sources: [makeSource()],
    jobs: [],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    ...overrides,
  };
}

test('unavailable sources or missing snapshots never report healthy zero counts', () => {
  const fromNull = projectTrayModel(null, { nowMs: NOW_MS });
  assert.equal(fromNull.status, 'unavailable');
  assert.equal(fromNull.workingCount, null);
  assert.equal(fromNull.attentionCount, null);
  assert.equal(fromNull.rows.length, 0);

  const fromError = projectTrayModel(makeSnapshot(), {
    nowMs: NOW_MS,
    error: 'Desktop bridge disconnected.',
  });
  assert.equal(fromError.status, 'unavailable');
  assert.equal(fromError.workingCount, null);
  assert.equal(fromError.attentionCount, null);

  const fromUnreachableSource = projectTrayModel(
    makeSnapshot({
      sources: [
        makeSource({
          availability: 'unavailable',
          message: 'State directory missing',
        }),
      ],
    }),
    { nowMs: NOW_MS }
  );
  assert.equal(fromUnreachableSource.status, 'unavailable');
  assert.equal(fromUnreachableSource.workingCount, null);
  assert.equal(fromUnreachableSource.attentionCount, null);

  const fromEmptyReachable = projectTrayModel(
    makeSnapshot({
      sources: [makeSource({ availability: 'available' })],
      jobs: [],
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
    }),
    { nowMs: NOW_MS }
  );
  assert.equal(fromEmptyReachable.status, 'empty');
  assert.equal(fromEmptyReachable.workingCount, 0);
  assert.equal(fromEmptyReachable.attentionCount, 0);
  assert.equal(fromEmptyReachable.rows.length, 0);
});

test('concurrent running work and pending human need are both preserved and counted independently', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-need-approval',
        title: 'Small-window minWidth update',
        state: 'needs_user',
        approval_status: 'pending_approval',
        execution_status: 'waiting_for_user',
        summary: 'Confirm lowering minWidth from 800px to 360px.',
        updated_at_ms: NOW_MS - 3_000,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-workers',
        job_id: null,
        state: 'running',
        children: [
          makeCall({
            id: 'call-worker-1',
            title: 'Build horizontal preview',
            state: 'running',
            started_at_ms: NOW_MS - 8_000,
          }),
          makeCall({
            id: 'call-worker-2',
            title: 'Build tray component',
            state: 'running',
            started_at_ms: NOW_MS - 6_000,
            executor: makeExecutor({
              tool: 'opencode',
              provider: 'opencode',
              requested_model: 'deepseek-v4.1-flash',
              observed_model: 'deepseek-v4.1-flash',
              requested_effort: 'max',
              observed_effort: 'max',
            }),
          }),
        ],
        children_count: 2,
      }),
    ],
  });

  const model = projectTrayModel(snapshot, { nowMs: NOW_MS });
  assert.equal(model.status, 'ready');
  assert.equal(model.workingCount, 2);
  assert.equal(model.attentionCount, 1);
  assert.equal(model.rows.length, 3);
  assert.equal(model.rows[0]?.category, 'needs_human');
  assert.equal(model.rows[0]?.title, 'Small-window minWidth update');
  assert.match(model.rows[0]?.detail ?? '', /minWidth/);
  assert.equal(model.rows[1]?.category, 'working');
  assert.equal(model.rows[2]?.category, 'working');
});

test('prioritizes needs-human over working over most recent result and preserves failed/unknown states', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-completed-old',
        title: 'Older completed cleanup',
        state: 'completed',
        approval_status: 'approved',
        execution_status: 'completed',
        updated_at_ms: NOW_MS - 120_000,
      }),
      makeJob({
        id: 'job-failed-latest',
        title: 'Telemetry verification gate',
        state: 'failed',
        approval_status: 'approved',
        execution_status: 'failed',
        summary: 'Missing observed effort evidence from provider.',
        updated_at_ms: NOW_MS - 10_000,
      }),
      makeJob({
        id: 'job-running',
        title: 'Active coder worktree',
        state: 'running',
        approval_status: 'approved',
        execution_status: 'running',
        active_call_count: 1,
        updated_at_ms: NOW_MS - 5_000,
      }),
      makeJob({
        id: 'job-unknown',
        title: 'Interrupted supervisor handoff',
        state: 'unknown',
        approval_status: 'approved',
        execution_status: 'unknown',
        updated_at_ms: NOW_MS - 30_000,
      }),
    ],
  });

  const model = projectTrayModel(snapshot, { nowMs: NOW_MS });
  assert.equal(model.status, 'ready');
  assert.equal(model.workingCount, 1);
  assert.equal(model.attentionCount, 1);
  assert.equal(model.rows.length, 3);

  // 1. Unknown state is surfaced in needs_human with unknown tone, never Completed
  assert.equal(model.rows[0]?.category, 'needs_human');
  assert.equal(model.rows[0]?.tone, 'unknown');
  assert.equal(model.rows[0]?.title, 'Interrupted supervisor handoff');
  assert.notEqual(model.rows[0]?.statusLabel, 'Completed');

  // 2. Working item comes second
  assert.equal(model.rows[1]?.category, 'working');
  assert.equal(model.rows[1]?.title, 'Active coder worktree');

  // 3. Most recent terminal result is the failed job (never relabeled Completed; older completed job excluded)
  assert.equal(model.rows[2]?.category, 'result');
  assert.equal(model.rows[2]?.tone, 'danger');
  assert.equal(model.rows[2]?.statusLabel, 'Failed');
  assert.equal(model.rows[2]?.title, 'Telemetry verification gate');
});

test('caps tray rows at 3 while retaining full header counts across overflow items', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-hold',
        title: 'Held migration scope',
        state: 'needs_user',
        approval_status: 'approved',
        execution_status: 'on_hold',
        updated_at_ms: NOW_MS - 25_000,
      }),
      makeJob({
        id: 'job-approval',
        title: 'Plan awaiting operator approval',
        state: 'ready',
        approval_status: 'pending_approval',
        execution_status: 'queued',
        updated_at_ms: NOW_MS - 15_000,
      }),
      makeJob({
        id: 'job-work-1',
        title: 'Older running worker',
        state: 'running',
        approval_status: 'approved',
        execution_status: 'running',
        active_call_count: 1,
        updated_at_ms: NOW_MS - 20_000,
      }),
      makeJob({
        id: 'job-work-2',
        title: 'Newest running worker',
        state: 'running',
        approval_status: 'approved',
        execution_status: 'running',
        active_call_count: 1,
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJob({
        id: 'job-done',
        title: 'Completed verification',
        state: 'completed',
        approval_status: 'approved',
        execution_status: 'completed',
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
  });

  const model = projectTrayModel(snapshot, { nowMs: NOW_MS });
  assert.equal(model.workingCount, 2);
  assert.equal(model.attentionCount, 2);
  assert.equal(model.rows.length, TRAY_MAX_ROWS);
  assert.deepEqual(
    model.rows.map((row) => [row.category, row.title]),
    [
      ['needs_human', 'Held migration scope'],
      ['needs_human', 'Plan awaiting operator approval'],
      ['working', 'Newest running worker'],
    ]
  );
});

test('coordinator coalesces duplicate in-flight reads and discards stale responses on forced reopen', async () => {
  let fetchCalls = 0;
  const resolvers: Array<(snap: ObservationSnapshot) => void> = [];

  const coordinator = new TrayObservationCoordinator(() => {
    fetchCalls += 1;
    return new Promise<ObservationSnapshot>((resolve) => {
      resolvers.push(resolve);
    });
  });

  // Duplicate poll while first read is in flight must not trigger a second fetch
  const firstPromise = coordinator.refresh();
  const duplicatePromise = coordinator.refresh();
  assert.equal(fetchCalls, 1);

  // Forced refresh (e.g. threadlane:tray-opened) queues exactly one follow-up and invalidates first
  const forcedPromise = coordinator.refresh({ force: true });
  assert.equal(fetchCalls, 1);

  const staleSnapshot = makeSnapshot({
    jobs: [makeJob({ id: 'job-stale', title: 'Stale job' })],
  });
  resolvers[0]!(staleSnapshot);

  // Resolving the first fetch starts the queued forced refresh without applying the stale snapshot
  await Promise.resolve();
  assert.equal(coordinator.getState().snapshot, null);
  assert.equal(fetchCalls, 2);

  const freshSnapshot = makeSnapshot({
    jobs: [makeJob({ id: 'job-fresh', title: 'Fresh job' })],
  });
  resolvers[1]!(freshSnapshot);
  await Promise.all([firstPromise, duplicatePromise, forcedPromise]);

  assert.equal(coordinator.getState().snapshot?.jobs[0]?.title, 'Fresh job');
  assert.equal(coordinator.getState().loading, false);

  coordinator.setVisibility(false, false);
  assert.equal(coordinator.canPoll(), false);
  coordinator.setVisibility(true, true);
  assert.equal(coordinator.canPoll(), true);
  coordinator.dispose();
  assert.equal(coordinator.canPoll(), false);
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
