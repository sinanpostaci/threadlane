import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type { DesktopSnapshot, TaskSummary } from '../types.ts';
import {
  MAX_VISIBLE_RECOVERY_ITEMS,
  projectRecoveryOverview,
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

function makeSnapshot(
  tasks: TaskSummary[],
  overrides: Partial<DesktopSnapshot> = {}
): DesktopSnapshot {
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
    tasks,
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

describe('recovery overview duplicate and invalid-input handling', () => {
  it('collapses duplicate entries for one selected-binding run ID into one visible attention entry', () => {
    const first = makeTask({
      run_id: 'run-duplicate',
      revision: 2,
      status: 'failed',
      updated_at_ms: NOW_MS - 2_000,
    });
    const duplicate = makeTask({
      run_id: 'run-duplicate',
      revision: 3,
      status: 'unknown',
      updated_at_ms: NOW_MS,
    });

    const projection = projectRecoveryOverview(makeSnapshot([first, duplicate]));

    assert.equal(projection.state, 'ready');
    assert.equal(projection.items.length, 1);
    assert.equal(projection.visibleItems.length, 1);
    assert.equal(projection.hiddenItemCount, 0);
    assert.equal(projection.attentionCount, 1);
    assert.equal(projection.activeCount, 0);
    assert.equal(projection.visibleItems[0]?.task.run_id, 'run-duplicate');
    assert.notEqual(projection.visibleItems[0]?.kind, 'active');
    assert.equal(
      projection.items.filter((item) => item.task.run_id === 'run-duplicate').length,
      1
    );
  });

  it('does not leak an identical run ID from another binding into the selected scope', () => {
    const foreignTask = makeTask({
      run_id: 'run-shared',
      binding_id: 'bind-beta',
      status: 'failed',
      title: 'Foreign run',
      updated_at_ms: NOW_MS + 5_000,
    });
    const selectedTask = makeTask({
      run_id: 'run-shared',
      binding_id: 'bind-alpha',
      status: 'unknown',
      title: 'Selected run',
      updated_at_ms: NOW_MS,
    });

    const projection = projectRecoveryOverview(
      makeSnapshot([foreignTask, selectedTask])
    );

    assert.equal(projection.bindingId, 'bind-alpha');
    assert.equal(projection.tasksInScope, 1);
    assert.equal(projection.items.length, 1);
    assert.equal(projection.visibleItems.length, 1);

    const entry = projection.visibleItems[0];
    assert.ok(entry);
    assert.equal(entry.task.binding_id, 'bind-alpha');
    assert.equal(entry.task.title, 'Selected run');
    assert.equal(entry.kind, 'unknown');
    assert.equal(entry.kindLabel, 'Uncertain outcome');
    assert.equal(
      projection.items.some((item) => item.task.binding_id === 'bind-beta'),
      false
    );
  });

  it('uses the existing bounded default for NaN, infinite, and nonpositive maxVisible values', () => {
    const tasks = Array.from({ length: MAX_VISIBLE_RECOVERY_ITEMS + 2 }, (_, index) =>
      makeTask({
        run_id: `run-limit-${index}`,
        status: 'failed',
        updated_at_ms: NOW_MS - index * 1_000,
      })
    );
    const invalidLimits = [
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      0,
      -1,
    ];

    for (const maxVisible of invalidLimits) {
      const projection = projectRecoveryOverview(makeSnapshot(tasks), { maxVisible });

      assert.equal(
        projection.visibleItems.length,
        MAX_VISIBLE_RECOVERY_ITEMS,
        `expected the default visible limit for maxVisible=${String(maxVisible)}`
      );
      assert.equal(
        projection.hiddenItemCount,
        tasks.length - MAX_VISIBLE_RECOVERY_ITEMS,
        `expected the remaining items to stay hidden for maxVisible=${String(maxVisible)}`
      );
      assert.equal(projection.bounded, true);
      assert.equal(projection.items.length, tasks.length);
    }
  });
});
