import test from 'node:test';
import assert from 'node:assert/strict';

import {
  projectTaskStatus,
  summarizeProjectTaskStatuses,
} from './projects-task-status.ts';

const aliases = [
  ['completed', 'Done', 'ok'],
  ['accepted', 'Done', 'ok'],
  ['historical_accepted', 'Done', 'ok'],
  ['integrated', 'Done', 'ok'],
  ['ready_for_review', 'Awaiting review', 'neutral'],
  ['pending_review', 'Awaiting review', 'neutral'],
  ['failed', 'Failed', 'danger'],
  ['timed_out', 'Timed out', 'danger'],
  ['cancelled', 'Stopped', 'cancelled'],
  ['canceled', 'Stopped', 'cancelled'],
  ['not_started', 'Not started', 'neutral'],
  ['pending_dependencies', 'Not started', 'neutral'],
  ['dependency_inputs_unverified', 'Not started', 'neutral'],
  ['dependency_inputs_ready', 'Not started', 'neutral'],
  ['native_route_required', 'Not started', 'neutral'],
  ['not_approved', 'Approval required', 'waiting'],
  ['pending_approval', 'Approval required', 'waiting'],
  ['running', 'Running', 'active'],
  ['claimed', 'Running', 'active'],
  ['queued', 'Queued', 'neutral'],
  ['pending', 'Queued', 'neutral'],
  ['held', 'On hold', 'waiting'],
  ['on_hold', 'On hold', 'waiting'],
] as const;

test('maps every exact alias to its label and tone', () => {
  for (const [status, label, tone] of aliases) {
    assert.deepEqual(projectTaskStatus(status), { label, tone });
  }
});

test('returns an unknown state for empty and unsupported states', () => {
  const unsupported = [
    'unknown',
    '',
    '   ',
    'mystery',
    'done',
    'in_progress',
    'constructor',
    'toString',
    '__proto__',
  ];

  for (const status of unsupported) {
    assert.deepEqual(projectTaskStatus(status), {
      label: 'Status unknown',
      tone: 'unknown',
    });
  }
});

test('keeps done, review, and not-started states distinct', () => {
  assert.deepEqual(projectTaskStatus('completed'), {
    label: 'Done',
    tone: 'ok',
  });
  assert.deepEqual(projectTaskStatus('historical_accepted'), {
    label: 'Done',
    tone: 'ok',
  });
  assert.deepEqual(projectTaskStatus('ready_for_review'), {
    label: 'Awaiting review',
    tone: 'neutral',
  });
  assert.deepEqual(projectTaskStatus('pending_review'), {
    label: 'Awaiting review',
    tone: 'neutral',
  });
  assert.deepEqual(projectTaskStatus('not_started'), {
    label: 'Not started',
    tone: 'neutral',
  });
  assert.notEqual(projectTaskStatus('ready_for_review').tone, 'ok');
  assert.notEqual(projectTaskStatus('not_started').tone, 'ok');
});

test('normalizes surrounding whitespace and case', () => {
  assert.deepEqual(projectTaskStatus('  CoMpLeTeD  '), {
    label: 'Done',
    tone: 'ok',
  });
  assert.deepEqual(projectTaskStatus('  ReAdY_FoR_ReViEw  '), {
    label: 'Awaiting review',
    tone: 'neutral',
  });
  assert.deepEqual(projectTaskStatus('  CANCELED  '), {
    label: 'Stopped',
    tone: 'cancelled',
  });
});

test('normalizes and groups aliases in summaries', () => {
  assert.equal(
    summarizeProjectTaskStatuses([
      { status: ' COMPLETED ' },
      { status: 'ACCEPTED' },
      { status: ' Ready_For_Review ' },
    ]),
    '1 awaiting review · 2 done',
  );
});

test('summarizes actual groups in deterministic count order', () => {
  const tasks = [
    { status: 'failed' },
    { status: 'timed_out' },
    { status: 'mystery' },
    { status: 'held' },
    { status: 'pending_approval' },
    { status: 'pending_review' },
    { status: 'claimed' },
    { status: 'queued' },
    { status: 'native_route_required' },
    { status: 'completed' },
    { status: 'accepted' },
    { status: 'canceled' },
  ];
  const expected =
    '1 failed · 1 timed out · 1 status unknown · 1 on hold · 1 approval required · 1 awaiting review · 1 running · 1 queued · 1 not started · 2 done · 1 stopped';

  assert.equal(summarizeProjectTaskStatuses(tasks), expected);
  assert.equal(summarizeProjectTaskStatuses([...tasks].reverse()), expected);
});

test('matches the requested failed and not-started example', () => {
  assert.equal(
    summarizeProjectTaskStatuses([
      { status: 'failed' },
      { status: 'failed' },
      { status: 'not_started' },
    ]),
    '2 failed · 1 not started',
  );
});

test('returns null for empty input and counts unknown only from records', () => {
  assert.equal(summarizeProjectTaskStatuses([]), null);
  assert.equal(
    summarizeProjectTaskStatuses([{ status: '' }]),
    '1 status unknown',
  );
});

test('does not mutate task records or their order', () => {
  const tasks = Object.freeze([
    Object.freeze({ status: 'failed' }),
    Object.freeze({ status: 'not_started' }),
    Object.freeze({ status: 'completed' }),
  ]);
  const before = tasks.map((task) => task.status);

  assert.equal(
    summarizeProjectTaskStatuses(tasks),
    '1 failed · 1 not started · 1 done',
  );
  assert.deepEqual(
    tasks.map((task) => task.status),
    before,
  );
});
