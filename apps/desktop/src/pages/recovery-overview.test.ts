import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type { DesktopSnapshot, TaskSummary } from '../types.ts';
import {
  MAX_VISIBLE_RECOVERY_ITEMS,
  classifyRecoveryTask,
  isRecoverySnapshotStale,
  projectRecoveryOverview,
  selectScopedRecoveryTasks,
} from './recovery-overview.ts';

const NOW_MS = 1_730_000_000_000;

function makeTask(overrides: Partial<TaskSummary> = {}): TaskSummary {
  return {
    run_id: 'run-1',
    binding_id: 'bind-alpha',
    revision: 1,
    status: 'running',
    title: 'Recorded task',
    provider: 'opencode',
    requested_model: 'deepseek-4.1-flash',
    requested_effort: 'high',
    observed_model: 'deepseek-4.1-flash',
    observed_effort: 'high',
    last_summary: null,
    updated_at_ms: NOW_MS,
    needs_evidence: false,
    kind: 'worker',
    profile_id: 'default',
    ...overrides,
  };
}

function makeSnapshot(overrides: Partial<DesktopSnapshot> = {}): DesktopSnapshot {
  return {
    attachment: {
      mode: 'runtime',
      connection: 'connected',
      state_dir: '/tmp/threadlane-state',
      message: null,
      updated_at_ms: NOW_MS,
    },
    bindings: [
      {
        binding_id: 'bind-alpha',
        thread_uuid: '00000000-0000-0000-0000-000000000001',
        label: 'Alpha chat',
        is_local_demo: false,
      },
      {
        binding_id: 'bind-beta',
        thread_uuid: '00000000-0000-0000-0000-000000000002',
        label: 'Beta chat',
        is_local_demo: false,
      },
    ],
    selected_binding_id: 'bind-alpha',
    tasks: [],
    total_runs: 0,
    active_runs: 0,
    has_more: false,
    demo_controls: {
      available: false,
      run_id: null,
      binding_id: null,
      can_answer: false,
      can_cancel: false,
      revision: null,
    },
    ...overrides,
  };
}

describe('recovery overview projection', () => {
  it('keeps unknown outcomes distinct from known running state without elapsed-time guessing', () => {
    const unknownTask = makeTask({
      run_id: 'run-unknown',
      status: 'unknown',
      updated_at_ms: NOW_MS - 3 * 24 * 60 * 60 * 1000,
    });
    const runningTask = makeTask({
      run_id: 'run-running',
      status: 'running',
      updated_at_ms: NOW_MS - 4 * 24 * 60 * 60 * 1000,
    });

    const unknown = classifyRecoveryTask(unknownTask);
    const running = classifyRecoveryTask(runningTask);
    assert.ok(unknown);
    assert.ok(running);
    assert.equal(unknown.kind, 'unknown');
    assert.match(unknown.nextStep, /inspect/i);
    assert.match(unknown.nextStep, /reconcile/i);
    assert.match(unknown.nextStep, /do not retry or resume/i);
    assert.doesNotMatch(
      `${unknown.kindLabel} ${unknown.detail} ${unknown.nextStep}`,
      /interrupted|died|dead/i
    );

    assert.equal(running.kind, 'active');
    assert.equal(running.kindLabel, 'Still running');
    assert.match(running.detail, /elapsed time/i);
    assert.doesNotMatch(
      `${running.kindLabel} ${running.detail} ${running.nextStep}`,
      /interrupted|unknown|failed|died|dead/i
    );
    assert.equal(
      classifyRecoveryTask(makeTask({ status: 'running', needs_evidence: true }))?.kind,
      'active'
    );

    const projection = projectRecoveryOverview(
      makeSnapshot({ tasks: [runningTask, unknownTask] })
    );
    assert.deepEqual(
      projection.items.map((item) => item.task.run_id),
      ['run-unknown', 'run-running']
    );
    assert.equal(projection.items[0]?.kind, 'unknown');
    assert.equal(projection.items[1]?.kind, 'active');
    assert.equal(projection.attentionCount, 1);
    assert.equal(projection.activeCount, 1);
  });

  it('treats completed/evidence review as review work and does not flood on clean completions', () => {
    const completedAwaitingEvidence = makeTask({
      run_id: 'run-review',
      status: 'completed',
      needs_evidence: true,
      observed_model: null,
      observed_effort: null,
    });
    const failedAwaitingEvidence = makeTask({
      run_id: 'run-failed-evidence',
      status: 'failed',
      needs_evidence: true,
      observed_effort: null,
    });
    const readyForReview = makeTask({
      run_id: 'run-ready',
      status: 'ready_for_review',
    });
    const cleanCompletion = makeTask({
      run_id: 'run-clean',
      status: 'completed',
    });

    const completedReview = classifyRecoveryTask(completedAwaitingEvidence);
    assert.ok(completedReview);
    assert.equal(completedReview.kind, 'review');
    assert.match(completedReview.detail, /not accepted as a success/i);
    assert.match(completedReview.nextStep, /evidence/i);
    assert.doesNotMatch(completedReview.kindLabel, /success|accepted/i);

    const failedReview = classifyRecoveryTask(failedAwaitingEvidence);
    assert.ok(failedReview);
    assert.equal(failedReview.kind, 'failed');
    assert.match(failedReview.detail, /known terminal execution/i);
    assert.notEqual(failedReview.kind, 'review');

    const pendingReview = classifyRecoveryTask(readyForReview);
    assert.ok(pendingReview);
    assert.equal(pendingReview.kind, 'review');
    assert.match(pendingReview.detail, /not acceptance/i);

    assert.equal(classifyRecoveryTask(cleanCompletion), null);

    const projection = projectRecoveryOverview(
      makeSnapshot({
        tasks: [
          cleanCompletion,
          completedAwaitingEvidence,
          failedAwaitingEvidence,
          readyForReview,
        ],
      })
    );
    assert.equal(projection.items.length, 3);
    assert.equal(projection.items.filter((item) => item.kind === 'review').length, 2);
    assert.equal(projection.items.find((item) => item.task.run_id === failedAwaitingEvidence.run_id)?.kind, 'failed');
    assert.equal(
      projection.items.some((item) => item.task.run_id === cleanCompletion.run_id),
      false
    );
  });

  it('keeps failed, timed-out, and cancelled execution distinct from uncertainty', () => {
    for (const status of ['failed', 'timed_out', 'cancelled', 'execution_failed']) {
      const classification = classifyRecoveryTask(makeTask({ status }));
      assert.ok(classification, `expected a recovery entry for ${status}`);
      assert.equal(classification.kind, 'failed', `expected failed kind for ${status}`);
      assert.doesNotMatch(classification.kindLabel, /unknown|uncertain/i);
    }

    for (const status of ['unknown', 'uncertain']) {
      const classification = classifyRecoveryTask(makeTask({ status }));
      assert.ok(classification, `expected a recovery entry for ${status}`);
      assert.equal(classification.kind, 'unknown', `expected unknown kind for ${status}`);
      assert.doesNotMatch(classification.kindLabel, /failed|timed out|cancelled/i);
    }
  });

  it('surfaces a waiting question as inspect-only attention', () => {
    const waiting = makeTask({
      status: 'waiting_input',
      last_summary: 'Which file should the worker inspect?',
    });
    const classification = classifyRecoveryTask(waiting);
    assert.ok(classification);
    assert.equal(classification.kind, 'waiting');
    assert.equal(classification.summary, 'Which file should the worker inspect?');
    assert.match(classification.nextStep, /linked codex conversation/i);
    assert.match(classification.nextStep, /does not send an answer/i);

    const projection = projectRecoveryOverview(makeSnapshot({ tasks: [waiting] }));
    assert.equal(projection.items.length, 1);
    assert.equal(projection.items[0]?.kind, 'waiting');
    assert.equal(projection.attentionCount, 1);
    assert.equal(projection.activeCount, 0);
  });

  it('shows stale status, disables inspection, and handles no data without inventing scope', () => {
    const unknownTask = makeTask({ status: 'unknown' });
    const staleProjection = projectRecoveryOverview(
      makeSnapshot({ tasks: [unknownTask] }),
      { stale: true }
    );
    assert.equal(staleProjection.stale, true);
    assert.equal(staleProjection.inspectEnabled, false);
    assert.match(staleProjection.statusLabel, /stale/i);
    assert.match(staleProjection.statusNote, /not live/i);
    assert.match(staleProjection.statusNote, /inspect is disabled/i);
    assert.equal(staleProjection.items[0]?.inspectable, false);

    const disconnected = makeSnapshot({
      attachment: {
        mode: 'runtime',
        connection: 'disconnected',
        state_dir: '/tmp/threadlane-state',
        message: 'observer offline',
        updated_at_ms: NOW_MS,
      },
    });
    assert.equal(isRecoverySnapshotStale(disconnected), true);
    const disconnectedProjection = projectRecoveryOverview(disconnected);
    assert.equal(disconnectedProjection.stale, true);
    assert.equal(disconnectedProjection.state, 'empty');
    assert.equal(disconnectedProjection.inspectEnabled, false);
    assert.match(disconnectedProjection.statusLabel, /stale/i);

    const noBinding = projectRecoveryOverview(
      makeSnapshot({ selected_binding_id: null, tasks: [unknownTask] })
    );
    assert.equal(noBinding.state, 'no_binding');
    assert.equal(noBinding.bindingId, null);
    assert.equal(noBinding.items.length, 0);
    assert.equal(noBinding.tasksInScope, 0);

    const loadingProjection = projectRecoveryOverview(makeSnapshot(), { loading: true });
    assert.equal(loadingProjection.state, 'loading');
    assert.match(loadingProjection.coverageNote, /when the snapshot loads/i);

    const emptyProjection = projectRecoveryOverview(makeSnapshot());
    assert.equal(emptyProjection.state, 'empty');
    assert.equal(emptyProjection.items.length, 0);
    assert.match(emptyProjection.coverageNote, /no recent runs/i);
  });

  it('bounds visible entries and reports snapshot coverage and hidden card items', () => {
    const tasks = Array.from({ length: 6 }, (_, index) =>
      makeTask({
        run_id: `run-${index}`,
        status: 'failed',
        updated_at_ms: NOW_MS - index * 1_000,
      })
    );
    const projection = projectRecoveryOverview(
      makeSnapshot({ tasks, total_runs: 9, has_more: true }),
      { maxVisible: 3 }
    );

    assert.equal(projection.items.length, 6);
    assert.equal(projection.visibleItems.length, 3);
    assert.equal(projection.hiddenItemCount, 3);
    assert.equal(projection.coverageBounded, true);
    assert.equal(projection.bounded, true);
    assert.match(projection.coverageNote, /recent runs only/i);
    assert.match(projection.coverageNote, /more may exist/i);
    assert.match(projection.coverageNote, /showing 6 of 9/i);

    const defaultBounded = projectRecoveryOverview(
      makeSnapshot({ tasks, total_runs: tasks.length })
    );
    assert.equal(defaultBounded.visibleItems.length, MAX_VISIBLE_RECOVERY_ITEMS);
    assert.equal(
      defaultBounded.hiddenItemCount,
      tasks.length - MAX_VISIBLE_RECOVERY_ITEMS
    );
    assert.equal(defaultBounded.coverageBounded, false);
    assert.equal(defaultBounded.bounded, true);
  });

  it('keeps has_more messaging honest even when the snapshot count is small', () => {
    const projection = projectRecoveryOverview(
      makeSnapshot({
        tasks: [makeTask({ status: 'failed' })],
        total_runs: 1,
        has_more: true,
      })
    );
    assert.equal(projection.hasMore, true);
    assert.equal(projection.coverageBounded, true);
    assert.match(projection.coverageNote, /recent runs only/i);
    assert.match(projection.coverageNote, /more may exist beyond this snapshot/i);
    assert.equal(projection.items.length, 1);

    const totalDrift = projectRecoveryOverview(
      makeSnapshot({
        tasks: [makeTask({ run_id: 'run-only', status: 'unknown' })],
        total_runs: 5,
        has_more: false,
      })
    );
    assert.equal(totalDrift.coverageBounded, true);
    assert.match(totalDrift.coverageNote, /more may exist/i);
    assert.equal(totalDrift.items.length, 1);
  });

  it('excludes tasks from a different binding and never fabricates tasks from totals', () => {
    const alphaTask = makeTask({ run_id: 'run-alpha', status: 'unknown' });
    const betaTask = makeTask({
      run_id: 'run-beta',
      binding_id: 'bind-beta',
      status: 'failed',
    });
    const snapshot = makeSnapshot({
      tasks: [alphaTask, betaTask],
      total_runs: 2,
      has_more: false,
    });

    assert.deepEqual(
      selectScopedRecoveryTasks(snapshot).map((task) => task.run_id),
      ['run-alpha']
    );
    const projection = projectRecoveryOverview(snapshot);
    assert.equal(projection.tasksInScope, 1);
    assert.equal(projection.items.length, 1);
    assert.equal(projection.items[0]?.task.binding_id, 'bind-alpha');
    assert.equal(
      projection.items.some((item) => item.task.binding_id === 'bind-beta'),
      false
    );

    const noBindingProjection = projectRecoveryOverview(
      makeSnapshot({
        selected_binding_id: null,
        tasks: [alphaTask, betaTask],
        total_runs: 2,
      })
    );
    assert.equal(noBindingProjection.items.length, 0);
    assert.equal(noBindingProjection.totalRuns, 0);
  });

  it('handles local demo runs as demo attention rather than worker recovery history', () => {
    const waitingDemo = makeTask({
      run_id: 'demo-waiting',
      kind: 'demo',
      status: 'waiting_input',
      provider: null,
      requested_model: null,
      requested_effort: null,
      observed_model: null,
      observed_effort: null,
      profile_id: null,
      last_summary: 'Confirm the next step?',
    });
    const completedDemo = makeTask({
      run_id: 'demo-completed',
      kind: 'demo',
      status: 'completed',
      provider: null,
      requested_model: null,
      requested_effort: null,
      observed_model: null,
      observed_effort: null,
      profile_id: null,
    });
    const cancelledDemo = makeTask({
      run_id: 'demo-cancelled',
      kind: 'demo',
      status: 'cancelled',
      provider: null,
      requested_model: null,
      requested_effort: null,
      observed_model: null,
      observed_effort: null,
      profile_id: null,
    });
    const runningDemo = makeTask({
      run_id: 'demo-running',
      kind: 'demo',
      status: 'running',
      provider: null,
      requested_model: null,
      requested_effort: null,
      observed_model: null,
      observed_effort: null,
      profile_id: null,
    });

    const waitingClassification = classifyRecoveryTask(waitingDemo);
    assert.ok(waitingClassification);
    assert.equal(waitingClassification.kind, 'waiting');
    assert.match(waitingClassification.detail, /local demo/i);
    assert.equal(waitingClassification.summary, 'Confirm the next step?');
    assert.equal(classifyRecoveryTask(completedDemo), null);
    assert.equal(classifyRecoveryTask(cancelledDemo), null);
    assert.equal(classifyRecoveryTask(runningDemo)?.kind, 'active');

    const projection = projectRecoveryOverview(
      makeSnapshot({
        attachment: {
          mode: 'demo',
          connection: 'connected',
          state_dir: '/tmp/threadlane-demo',
          message: null,
          updated_at_ms: NOW_MS,
        },
        tasks: [completedDemo, waitingDemo, cancelledDemo],
      })
    );
    assert.equal(projection.isDemoScope, true);
    assert.equal(projection.items.length, 1);
    assert.equal(projection.items[0]?.task.run_id, 'demo-waiting');
    assert.equal(projection.items[0]?.isDemo, true);
  });
});
