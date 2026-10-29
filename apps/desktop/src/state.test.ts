import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  applyMutationError,
  applyMutationSuccess,
  applyRunDetailError,
  applyRunDetailSuccess,
  applySnapshotPollError,
  applySnapshotPollSuccess,
  beginMutation,
  beginRunDetailRefresh,
  beginSnapshotPoll,
  createInitialSnapshot,
  createInitialState,
  navigateBack,
  navigateToPage,
  openRunDetail,
  resolveExpectedDemoRevision,
  selectBinding,
} from './state.ts';
import {
  classifyTaskBucket,
  deriveRecordedProfiles,
  formatAttachmentBadge,
  formatEvidenceFieldCheck,
  formatEvidenceSummary,
  formatModelAndEffort,
  formatStatusLabel,
  isTaskActive,
  matchesProviderFilter,
  resolveTaskBrand,
  sanitizeAppearancePreferences,
} from './format.ts';
import type { DesktopSnapshot, QueueBucket, RunDetail, TaskKind, TaskSummary } from './types.ts';

function makeTask(overrides: Partial<TaskSummary> = {}): TaskSummary {
  return {
    run_id: 'run-1',
    binding_id: 'bind-alpha',
    revision: 2,
    status: 'Running',
    title: 'Verify worktree isolation',
    provider: 'antigravity',
    requested_model: 'gemini-4-argon-high',
    requested_effort: 'high',
    observed_model: 'gemini-4-argon-high',
    observed_effort: 'high',
    last_summary: 'Editing scoped target',
    updated_at_ms: 1730000000000,
    needs_evidence: false,
    kind: 'worker',
    profile_id: 'default',
    ...overrides,
  };
}

function makeSnapshot(overrides: Partial<DesktopSnapshot> = {}): DesktopSnapshot {
  const base = createInitialSnapshot(1730000000000);
  return {
    ...base,
    attachment: {
      mode: 'runtime',
      connection: 'connected',
      state_dir: '/tmp/threadlane-state',
      message: null,
      updated_at_ms: 1730000000000,
    },
    bindings: [
      {
        binding_id: 'bind-alpha',
        thread_uuid: '00000000-0000-0000-0000-000000000001',
        label: 'Core Runtime',
        is_local_demo: false,
      },
      {
        binding_id: 'bind-beta',
        thread_uuid: '00000000-0000-0000-0000-000000000002',
        label: 'Desktop Panel',
        is_local_demo: false,
      },
    ],
    selected_binding_id: 'bind-alpha',
    tasks: [makeTask()],
    total_runs: 1,
    active_runs: 1,
    has_more: false,
    ...overrides,
  };
}

function makeDetail(task: TaskSummary, overrides: Partial<RunDetail> = {}): RunDetail {
  return {
    task,
    instruction: 'Execute isolated verification checks.',
    outcome_summary: null,
    failure_reason: null,
    acceptance_state: 'Pending',
    exit_code: null,
    branch_name: 'task/run-1',
    worktree_path: '/tmp/worktree/run-1',
    latest_activity: 'Running unit tests',
    question: null,
    counts: {
      durable_progress_events: 4,
      coalesced_progress_events: 1,
      dropped_live_events: 0,
    },
    missing_model_evidence: false,
    missing_effort_evidence: false,
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

describe('snapshot polling & stale reply rejection', () => {
  it('prevents overlapping snapshot polls when one is already in flight', () => {
    const initial = createInitialState();
    const first = beginSnapshotPoll(initial);
    assert.ok(first.ticket !== null);
    assert.equal(first.state.pollInFlight, true);

    const overlapping = beginSnapshotPoll(first.state);
    assert.equal(overlapping.ticket, null);
    assert.equal(overlapping.state.snapshotRequestSeq, first.state.snapshotRequestSeq);
  });

  it('rejects a late snapshot poll reply after binding selection changes', () => {
    const initial = createInitialState();
    const poll = beginSnapshotPoll(initial);
    assert.ok(poll.ticket !== null);

    // User switches binding before the poll resolves
    const switched = selectBinding(poll.state, 'bind-beta', true);
    assert.ok(switched.ticket !== null);
    assert.equal(switched.state.selectedBindingId, 'bind-beta');
    assert.ok(switched.state.selectionEpoch > poll.ticket.epoch);

    // Stale reply for bind-alpha arrives late
    const staleSnapshot = makeSnapshot({ selected_binding_id: 'bind-alpha' });
    const afterStale = applySnapshotPollSuccess(switched.state, poll.ticket, staleSnapshot);
    assert.equal(afterStale.selectedBindingId, 'bind-beta');
    assert.equal(afterStale.snapshot.tasks.length, 0);

    // Fresh reply for bind-beta arrives
    const freshTask = makeTask({ run_id: 'run-beta', binding_id: 'bind-beta' });
    const freshSnapshot = makeSnapshot({
      selected_binding_id: 'bind-beta',
      tasks: [freshTask],
    });
    const afterFresh = applySnapshotPollSuccess(afterStale, switched.ticket, freshSnapshot);
    assert.equal(afterFresh.selectedBindingId, 'bind-beta');
    assert.equal(afterFresh.snapshot.tasks[0]?.run_id, 'run-beta');
    assert.equal(afterFresh.pollInFlight, false);
  });

  it('serializes binding selection while mutation is pending so success and error completion never get stuck', () => {
    // Case 1: attempted binding selection while start_demo mutation is pending, followed by mutation success
    const initial = createInitialState();
    const started = beginMutation(initial, 'start_demo');
    assert.ok(started.ticket !== null);
    assert.equal(started.state.pendingMutation, 'start_demo');

    const attemptedSwitch = selectBinding(started.state, 'bind-beta', true);
    assert.equal(attemptedSwitch.ticket, null);
    assert.equal(attemptedSwitch.state.selectionEpoch, started.ticket.epoch);
    assert.equal(attemptedSwitch.state.pendingMutation, 'start_demo');

    const demoSnapshot = makeSnapshot({ selected_binding_id: 'bind-alpha' });
    const afterSuccess = applyMutationSuccess(
      attemptedSwitch.state,
      started.ticket,
      demoSnapshot
    );
    assert.equal(afterSuccess.pendingMutation, null);
    assert.equal(afterSuccess.selectedBindingId, 'bind-alpha');

    // Confirm future poll, binding selection, and mutation are all allowed after completion
    const nextPoll = beginSnapshotPoll(afterSuccess);
    assert.ok(nextPoll.ticket !== null);
    const nextSwitch = selectBinding(afterSuccess, 'bind-beta', true);
    assert.ok(nextSwitch.ticket !== null);
    assert.equal(nextSwitch.state.selectedBindingId, 'bind-beta');

    // Case 2: attempted binding selection while attach mutation is pending, followed by mutation error
    const attach = beginMutation(afterSuccess, 'attach');
    assert.ok(attach.ticket !== null);
    const blockedSwitch = selectBinding(attach.state, 'bind-beta', false);
    assert.equal(blockedSwitch.ticket, null);
    assert.equal(blockedSwitch.state.selectionEpoch, attach.ticket.epoch);

    const afterError = applyMutationError(
      blockedSwitch.state,
      attach.ticket,
      'State directory does not exist.'
    );
    assert.equal(afterError.pendingMutation, null);
    assert.equal(afterError.actionError, 'State directory does not exist.');

    const pollAfterError = beginSnapshotPoll(afterError);
    assert.ok(pollAfterError.ticket !== null);
    const mutAfterError = beginMutation(afterError, 'cancel_demo');
    assert.ok(mutAfterError.ticket !== null);
  });

  it('rejects a late snapshot poll reply that arrives during or after attach mutation', () => {
    const initial = createInitialState();
    const poll = beginSnapshotPoll(initial);
    assert.ok(poll.ticket !== null);

    const attach = beginMutation(poll.state, 'attach');
    assert.ok(attach.ticket !== null);

    // Duplicate mutation while one is pending is blocked
    const duplicate = beginMutation(attach.state, 'start_demo');
    assert.equal(duplicate.ticket, null);

    // Poll reply arriving while attach is in flight is ignored
    const staleSnapshot = makeSnapshot({ selected_binding_id: 'bind-alpha' });
    const duringAttach = applySnapshotPollSuccess(attach.state, poll.ticket, staleSnapshot);
    assert.equal(duringAttach.snapshot.bindings.length, 0);

    // Attach succeeds and switches view to attached runtime
    const attachedSnapshot = makeSnapshot({
      selected_binding_id: 'bind-beta',
      attachment: {
        mode: 'runtime',
        connection: 'connected',
        state_dir: '/var/run/threadlane',
        message: null,
        updated_at_ms: 1730000005000,
      },
    });
    const afterAttach = applyMutationSuccess(duringAttach, attach.ticket, attachedSnapshot);
    assert.equal(afterAttach.pendingMutation, null);
    assert.equal(afterAttach.selectedBindingId, 'bind-beta');
    assert.equal(afterAttach.stateDirDraft, '/var/run/threadlane');
  });
});

describe('run detail race & stale reply guards', () => {
  it('ignores a late run_detail reply when user switches to a different run or closes detail', () => {
    let state = createInitialState();
    const firstOpen = openRunDetail(state, 'bind-alpha', 'run-1');
    state = firstOpen.state;

    // User immediately opens run-2 before run-1 detail returns
    const secondOpen = openRunDetail(state, 'bind-alpha', 'run-2');
    state = secondOpen.state;

    const detailRun1 = makeDetail(makeTask({ run_id: 'run-1', binding_id: 'bind-alpha' }));
    const afterLateFirst = applyRunDetailSuccess(state, firstOpen.ticket, detailRun1);
    assert.equal(afterLateFirst.runDetail, null);
    assert.equal(afterLateFirst.detailLoading, true);

    const detailRun2 = makeDetail(makeTask({ run_id: 'run-2', binding_id: 'bind-alpha' }));
    const afterSecond = applyRunDetailSuccess(afterLateFirst, secondOpen.ticket, detailRun2);
    assert.equal(afterSecond.runDetail?.task.run_id, 'run-2');
    assert.equal(afterSecond.detailLoading, false);

    // Navigating back closes detail and invalidates any subsequent late refresh
    const refresh = beginRunDetailRefresh(afterSecond);
    assert.ok(refresh.ticket !== null);
    const closed = navigateBack(refresh.state);
    assert.equal(closed.detailTarget, null);

    const afterClosedReply = applyRunDetailError(closed, refresh.ticket, 'Late error');
    assert.equal(afterClosedReply.detailError, null);
    assert.equal(afterClosedReply.detailTarget, null);
  });

  it('clears incompatible run detail when switching to another conversation binding', () => {
    const initial = createInitialState();
    const opened = openRunDetail(initial, 'bind-alpha', 'run-1');
    const detail = makeDetail(makeTask({ run_id: 'run-1', binding_id: 'bind-alpha' }));
    const withDetail = applyRunDetailSuccess(opened.state, opened.ticket, detail);
    assert.equal(withDetail.detailTarget?.runId, 'run-1');

    const switched = selectBinding(withDetail, 'bind-beta', true);
    assert.equal(switched.state.detailTarget, null);
    assert.equal(switched.state.runDetail, null);

    const tabSwitched = navigateToPage(withDetail, 'tools');
    assert.equal(tabSwitched.detailTarget, null);
    assert.equal(tabSwitched.page, 'tools');
  });
});

describe('demo controls & expected revision handling', () => {
  it('prefers detail demo_controls revision when open and falls back to snapshot revision', () => {
    let state = createInitialState();
    const demoSnapshot = makeSnapshot({
      demo_controls: {
        available: true,
        run_id: 'demo-run',
        binding_id: 'demo-bind',
        can_answer: true,
        can_cancel: true,
        revision: 3,
      },
    });
    const poll = beginSnapshotPoll(state);
    assert.ok(poll.ticket !== null);
    state = applySnapshotPollSuccess(poll.state, poll.ticket, demoSnapshot);
    assert.equal(resolveExpectedDemoRevision(state), 3);

    const opened = openRunDetail(state, 'demo-bind', 'demo-run');
    const detail = makeDetail(
      makeTask({ run_id: 'demo-run', binding_id: 'demo-bind', revision: 4, kind: 'demo' }),
      {
        demo_controls: {
          available: true,
          run_id: 'demo-run',
          binding_id: 'demo-bind',
          can_answer: false,
          can_cancel: true,
          revision: 4,
        },
      }
    );
    state = applyRunDetailSuccess(opened.state, opened.ticket, detail);
    assert.equal(resolveExpectedDemoRevision(state), 4);
  });

  it('surfaces safe error message when demo mutation fails with stale revision', () => {
    const initial = createInitialState();
    const mut = beginMutation(initial, 'answer_demo');
    assert.ok(mut.ticket !== null);
    const failed = applyMutationError(
      mut.state,
      mut.ticket,
      'Demo revision mismatch: expected 2, current is 3.'
    );
    assert.equal(failed.pendingMutation, null);
    assert.equal(failed.actionError, 'Demo revision mismatch: expected 2, current is 3.');

    // Stale poll error does not overwrite state after mutation epoch advanced
    const staleTicket = { seq: 0, epoch: 1, requestedBindingId: null };
    const afterStaleErr = applySnapshotPollError(failed, staleTicket, 'Old network error');
    assert.equal(afterStaleErr.actionError, 'Demo revision mismatch: expected 2, current is 3.');
  });

  it('routes demo start/answer/cancel to Activity and expands Finished while keeping attach behavior unchanged', () => {
    let state = navigateToPage(createInitialState(), 'settings');
    assert.equal(state.page, 'settings');
    assert.equal(state.completedExpanded, false);

    // Attach from settings routes to Activity but preserves default collapsed Finished state
    const attach = beginMutation(state, 'attach');
    assert.ok(attach.ticket !== null);
    state = applyMutationSuccess(
      attach.state,
      attach.ticket,
      makeSnapshot({ selected_binding_id: 'bind-beta' })
    );
    assert.equal(state.page, 'activity');
    assert.equal(state.completedExpanded, false);
    assert.equal(state.selectedBindingId, 'bind-beta');

    // Starting demo from Settings routes to Activity and expands Finished
    state = navigateToPage(state, 'settings');
    const startDemo = beginMutation(state, 'start_demo');
    assert.ok(startDemo.ticket !== null);
    state = applyMutationSuccess(
      startDemo.state,
      startDemo.ticket,
      makeSnapshot({
        selected_binding_id: 'bind-alpha',
        tasks: [makeTask({ run_id: 'demo-1', status: 'waiting_input', kind: 'demo' })],
      })
    );
    assert.equal(state.page, 'activity');
    assert.equal(state.completedExpanded, true);
    assert.equal(state.selectedBindingId, 'bind-alpha');

    // Even if user collapsed Finished or navigated away, answer_demo and cancel_demo show Activity and expand Finished
    state = { ...navigateToPage(state, 'chats'), completedExpanded: false };
    const answerDemo = beginMutation(state, 'answer_demo');
    assert.ok(answerDemo.ticket !== null);
    state = applyMutationSuccess(
      answerDemo.state,
      answerDemo.ticket,
      makeSnapshot({
        selected_binding_id: 'bind-alpha',
        tasks: [makeTask({ run_id: 'demo-1', status: 'completed', kind: 'demo' })],
      })
    );
    assert.equal(state.page, 'activity');
    assert.equal(state.completedExpanded, true);

    state = { ...navigateToPage(state, 'settings'), completedExpanded: false };
    const cancelDemo = beginMutation(state, 'cancel_demo');
    assert.ok(cancelDemo.ticket !== null);
    state = applyMutationSuccess(
      cancelDemo.state,
      cancelDemo.ticket,
      makeSnapshot({
        selected_binding_id: 'bind-alpha',
        tasks: [makeTask({ run_id: 'demo-1', status: 'cancelled', kind: 'demo' })],
      })
    );
    assert.equal(state.page, 'activity');
    assert.equal(state.completedExpanded, true);
  });
});

describe('task classification, profile derivation & appearance validation', () => {
  it('classifies actual core lifecycle states and fixture states accurately', () => {
    const coreLifecycleTable: ReadonlyArray<{
      status: string;
      bucket: QueueBucket;
      active: boolean;
      label: string;
    }> = [
      { status: 'queued', bucket: 'active', active: true, label: 'Queued' },
      { status: 'claimed', bucket: 'active', active: true, label: 'Claimed' },
      { status: 'running', bucket: 'active', active: true, label: 'Running' },
      { status: 'waiting_input', bucket: 'waiting', active: true, label: 'Waiting for input' },
      { status: 'completing', bucket: 'active', active: true, label: 'Completing' },
      { status: 'cancel_requested', bucket: 'active', active: true, label: 'Cancel requested' },
      { status: 'completed', bucket: 'completed', active: false, label: 'Completed' },
      { status: 'failed', bucket: 'completed', active: false, label: 'Failed' },
      { status: 'cancelled', bucket: 'completed', active: false, label: 'Cancelled' },
      { status: 'timed_out', bucket: 'completed', active: false, label: 'Timed out' },
      { status: 'unknown', bucket: 'completed', active: false, label: 'Unknown (uncertain)' },
    ];

    for (const entry of coreLifecycleTable) {
      const task = makeTask({ status: entry.status });
      assert.equal(
        classifyTaskBucket(task),
        entry.bucket,
        `expected bucket ${entry.bucket} for status ${entry.status}`
      );
      assert.equal(
        isTaskActive(task),
        entry.active,
        `expected isTaskActive=${entry.active} for status ${entry.status}`
      );
      const label = formatStatusLabel(entry.status);
      assert.equal(label, entry.label, `expected label ${entry.label} for status ${entry.status}`);
      assert.ok(
        !/succeeded|successful|accepted/i.test(label),
        `terminal status label "${label}" must not claim success or acceptance`
      );
    }

    assert.equal(classifyTaskBucket(makeTask({ status: 'WaitingForAnswer' })), 'waiting');
    assert.equal(classifyTaskBucket(makeTask({ status: 'Running' })), 'active');
    assert.equal(classifyTaskBucket(makeTask({ status: 'ExecutionFailed' })), 'completed');
    assert.equal(classifyTaskBucket(makeTask({ status: 'Completed' })), 'completed');
  });

  it('maps provider brands and filters accurately', () => {
    const agyTask = makeTask({ provider: 'antigravity', observed_model: 'gemini-4-argon-high' });
    const ocTask = makeTask({
      provider: 'opencode',
      observed_model: 'deepseek-4.1-flash',
      requested_model: 'deepseek-4.1-flash',
    });
    const demoTask = makeTask({ provider: null, kind: 'demo', observed_model: null, requested_model: null });

    assert.equal(resolveTaskBrand(agyTask), 'gemini');
    assert.equal(resolveTaskBrand(ocTask), 'deepseek');
    assert.equal(resolveTaskBrand(demoTask), 'demo');

    assert.equal(matchesProviderFilter(agyTask, 'antigravity'), true);
    assert.equal(matchesProviderFilter(agyTask, 'opencode'), false);
    assert.equal(matchesProviderFilter(ocTask, 'opencode'), true);
    assert.equal(matchesProviderFilter(demoTask, 'demo'), true);
  });

  it('derives read-only profile groups from recorded task profile IDs', () => {
    const tasks: TaskSummary[] = [
      makeTask({ run_id: 'r1', profile_id: 'default', status: 'Running' }),
      makeTask({
        run_id: 'r2',
        profile_id: 'default',
        status: 'ExecutionFailed',
        needs_evidence: true,
        observed_effort: null,
      }),
      makeTask({ run_id: 'r3', profile_id: null, kind: 'demo', status: 'Completed' }),
    ];

    const profiles = deriveRecordedProfiles(tasks);
    assert.equal(profiles.length, 2);
    assert.equal(profiles[0]?.profileId, 'default');
    assert.equal(profiles[0]?.runCount, 2);
    assert.equal(profiles[0]?.activeCount, 1);
    assert.equal(profiles[0]?.needsEvidenceCount, 1);
    assert.equal(profiles[1]?.isUnassigned, true);
  });

  it('formats run evidence labels accurately across worker, demo, and unknown tasks', () => {
    const evidenceTable: ReadonlyArray<{
      label: string;
      kind: TaskKind;
      status: string;
      observedModel: string | null;
      observedEffort: string | null;
      needsEvidence: boolean;
      missingModelFlag: boolean;
      missingEffortFlag: boolean;
      expected: string;
      forbidRecorded: boolean;
    }> = [
      {
        label: 'queued worker with no observed values (even with requested values)',
        kind: 'worker',
        status: 'queued',
        observedModel: null,
        observedEffort: null,
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Pending (unverified)',
        forbidRecorded: true,
      },
      {
        label: 'unknown status worker with no observed values',
        kind: 'worker',
        status: 'unknown',
        observedModel: null,
        observedEffort: null,
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Unverified (not observed)',
        forbidRecorded: true,
      },
      {
        label: 'unknown kind task even with observed strings',
        kind: 'unknown',
        status: 'completed',
        observedModel: 'gemini-4-argon-high',
        observedEffort: 'high',
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Unverified',
        forbidRecorded: true,
      },
      {
        label: 'running worker missing observed_model',
        kind: 'worker',
        status: 'running',
        observedModel: null,
        observedEffort: 'high',
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Pending (model unverified)',
        forbidRecorded: true,
      },
      {
        label: 'completed worker missing observed_model without explicit flag',
        kind: 'worker',
        status: 'completed',
        observedModel: null,
        observedEffort: 'high',
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Unverified (model not observed)',
        forbidRecorded: true,
      },
      {
        label: 'running worker missing observed_effort',
        kind: 'worker',
        status: 'running',
        observedModel: 'gemini-4-argon-high',
        observedEffort: null,
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Pending (effort unverified)',
        forbidRecorded: true,
      },
      {
        label: 'completed worker missing observed_effort without explicit flag',
        kind: 'worker',
        status: 'completed',
        observedModel: 'gemini-4-argon-high',
        observedEffort: null,
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Unverified (effort not observed)',
        forbidRecorded: true,
      },
      {
        label: 'worker with both nonempty observed model and effort',
        kind: 'worker',
        status: 'completed',
        observedModel: 'gemini-4-argon-high',
        observedEffort: 'high',
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Model & effort recorded',
        forbidRecorded: false,
      },
      {
        label: 'local demo without missing flags',
        kind: 'demo',
        status: 'waiting_input',
        observedModel: null,
        observedEffort: null,
        needsEvidence: false,
        missingModelFlag: false,
        missingEffortFlag: false,
        expected: 'Not applicable (local demo)',
        forbidRecorded: true,
      },
      {
        label: 'demo or task with explicit missing model and effort flags',
        kind: 'demo',
        status: 'failed',
        observedModel: null,
        observedEffort: null,
        needsEvidence: true,
        missingModelFlag: true,
        missingEffortFlag: true,
        expected: 'Missing model & effort evidence',
        forbidRecorded: true,
      },
      {
        label: 'worker with explicit missing model flag',
        kind: 'worker',
        status: 'failed',
        observedModel: null,
        observedEffort: 'high',
        needsEvidence: true,
        missingModelFlag: true,
        missingEffortFlag: false,
        expected: 'Missing model evidence',
        forbidRecorded: true,
      },
      {
        label: 'worker with explicit missing effort flag',
        kind: 'worker',
        status: 'failed',
        observedModel: 'deepseek-4.1-flash',
        observedEffort: null,
        needsEvidence: true,
        missingModelFlag: false,
        missingEffortFlag: true,
        expected: 'Missing effort evidence',
        forbidRecorded: true,
      },
    ];

    for (const row of evidenceTable) {
      const task = makeTask({
        kind: row.kind,
        status: row.status,
        requested_model: 'requested-model-is-not-proof',
        requested_effort: 'high',
        observed_model: row.observedModel,
        observed_effort: row.observedEffort,
        needs_evidence: row.needsEvidence,
      });
      const actual = formatEvidenceSummary(task, row.missingModelFlag, row.missingEffortFlag);
      assert.equal(actual, row.expected, `failed evidence case: ${row.label}`);
      if (row.forbidRecorded) {
        assert.ok(
          !/recorded/i.test(actual),
          `case "${row.label}" must not claim recorded evidence, got "${actual}"`
        );
      }
    }

    const queuedTask = makeTask({
      kind: 'worker',
      status: 'queued',
      observed_model: null,
      observed_effort: null,
    });
    assert.equal(formatEvidenceFieldCheck(queuedTask, null, false, 'model'), 'Pending (unverified)');
    const demoTask = makeTask({ kind: 'demo', status: 'running', observed_model: null });
    assert.equal(
      formatEvidenceFieldCheck(demoTask, null, false, 'effort'),
      'Not applicable (local demo)'
    );
  });

  it('formats observed vs requested model telemetry and clamps appearance preferences', () => {
    const unverifiedEffort = makeTask({
      observed_model: 'gemini-4-argon-high',
      observed_effort: null,
      requested_effort: 'high',
    });
    assert.equal(formatModelAndEffort(unverifiedEffort), 'gemini-4-argon-high · high (req)');

    const prefs = sanitizeAppearancePreferences({
      theme: 'dark',
      surface: 'glass',
      opacity: 40,
      accent: 'violet',
      rememberStateDir: false,
      savedStateDir: '/should/not/persist/when/disabled',
    });
    assert.equal(prefs.theme, 'dark');
    assert.equal(prefs.opacity, 65);
    assert.equal(prefs.accent, 'violet');
    assert.equal(prefs.savedStateDir, '');

    assert.equal(
      formatAttachmentBadge(
        {
          mode: 'demo',
          connection: 'connected',
          state_dir: '/tmp/demo',
          message: null,
          updated_at_ms: 1,
        },
        false
      ),
      'Local demo'
    );
  });
});
