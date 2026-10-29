import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  MAX_RETAINED_HISTORY_EVENTS,
  applyHistoryPageError,
  applyHistoryPageSuccess,
  applyMutationSuccess,
  applySnapshotPollSuccess,
  beginHistoryQuery,
  beginMutation,
  beginSnapshotPoll,
  commitHistoryFilters,
  createInitialSnapshot,
  createInitialState,
  navigateToPage,
  openRunDetail,
  resetHistoryFilters,
  selectBinding,
  selectHistoryEvent,
  setHistoryExpanded,
  shouldPollHistory,
  toActivityHistoryIpcArgs,
  updateHistoryRunIdDraft,
  type DesktopStoreState,
} from '../state.ts';
import {
  formatEventKindLabel,
  formatHistoryTimestamp,
  isValidEventKind,
  resolveEventKindTone,
} from './history.ts';
import { ipcDesktopActivityHistory } from '../ipc.ts';
import type { ActivityEvent, ActivityPage, DesktopSnapshot, EventKind } from '../types.ts';

function makeEvent(
  seq: number,
  overrides: Partial<ActivityEvent> = {}
): ActivityEvent {
  return {
    event_id: `evt-${seq}`,
    binding_id: 'bind-alpha',
    seq,
    run_id: 'run-1',
    revision: 1,
    step_seq: seq,
    kind: 'worker_progress',
    important: false,
    payload: {
      untrusted_data: true,
      summary: `Progress step ${seq}`,
      question_id: null,
      question_prompt: null,
      answer: null,
      instruction: null,
      cancel_reason: null,
    },
    created_at_ms: 1730000000000 + seq * 1000,
    ...overrides,
  };
}

function makeConnectedSnapshot(
  overrides: Partial<DesktopSnapshot> = {}
): DesktopSnapshot {
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
    ...overrides,
  };
}

function createConnectedActivityState(): DesktopStoreState {
  let state = createInitialState();
  const poll = beginSnapshotPoll(state);
  assert.ok(poll.ticket !== null);
  state = applySnapshotPollSuccess(poll.state, poll.ticket, makeConnectedSnapshot());
  state = setHistoryExpanded(state, true);
  return state;
}

describe('Activity History subview state, pagination, filters & polling isolation', () => {
  it('binds queryEpoch to attachment + binding + committed filters and discards stale query replies', () => {
    let state = createConnectedActivityState();
    const initialEpoch = state.history.queryEpoch;

    // Start an initial query on bind-alpha
    const q1 = beginHistoryQuery(state, 'initial');
    assert.ok(q1.ticket !== null);
    state = q1.state;
    assert.equal(q1.ticket.bindingId, 'bind-alpha');
    assert.equal(q1.ticket.queryEpoch, initialEpoch);

    // 1. Typing in runIdDraft does NOT increment queryEpoch or invalidate in-flight query
    state = updateHistoryRunIdDraft(state, 'run-special');
    assert.equal(state.history.runIdDraft, 'run-special');
    assert.equal(state.history.runIdFilter, null);
    assert.equal(state.history.queryEpoch, initialEpoch);
    assert.equal(state.history.inFlight, true);

    // 2. Committing runIdFilter increments queryEpoch and resets pagination window
    state = commitHistoryFilters(state);
    assert.equal(state.history.runIdFilter, 'run-special');
    assert.ok(state.history.queryEpoch > initialEpoch);
    assert.equal(state.history.inFlight, false);

    // Stale reply from q1 (before filter was committed) must be discarded!
    const stalePage: ActivityPage = {
      binding_id: 'bind-alpha',
      events: [makeEvent(1, { run_id: 'run-other' })],
      next_cursor: null,
      has_more: false,
      high_water_mark_seq: 1,
      after_seq: 0,
    };
    state = applyHistoryPageSuccess(state, q1.ticket, stalePage);
    assert.equal(state.history.events.length, 0);
    assert.equal(state.history.highWaterMarkSeq, 0);

    // 3. Changing kindFilter increments queryEpoch
    const epochAfterRunFilter = state.history.queryEpoch;
    state = commitHistoryFilters(state, undefined, 'worker_completed');
    assert.equal(state.history.kindFilter, 'worker_completed');
    assert.ok(state.history.queryEpoch > epochAfterRunFilter);

    // 4. Switching binding via selectBinding increments queryEpoch and discards in-flight queries
    const q2 = beginHistoryQuery(state, 'initial');
    assert.ok(q2.ticket !== null);
    state = q2.state;
    const epochBeforeSwitch = state.history.queryEpoch;

    const switched = selectBinding(state, 'bind-beta', true);
    state = switched.state;
    assert.ok(state.history.queryEpoch > epochBeforeSwitch);
    assert.equal(state.history.inFlight, false);

    state = applyHistoryPageError(state, q2.ticket, 'Late error from bind-alpha');
    assert.equal(state.history.error, null);

    // 5. Attachment state_dir change via attach mutation increments queryEpoch
    const epochBeforeAttach = state.history.queryEpoch;
    const attach = beginMutation(state, 'attach');
    assert.ok(attach.ticket !== null);
    state = applyMutationSuccess(
      attach.state,
      attach.ticket,
      makeConnectedSnapshot({
        selected_binding_id: 'bind-beta',
        attachment: {
          mode: 'runtime',
          connection: 'connected',
          state_dir: '/tmp/other-state-dir',
          message: null,
          updated_at_ms: 1730000010000,
        },
      })
    );
    assert.ok(state.history.queryEpoch > epochBeforeAttach);

    // 6. Resetting filters clears both runIdFilter and kindFilter and advances queryEpoch
    const epochBeforeReset = state.history.queryEpoch;
    state = resetHistoryFilters(state);
    assert.equal(state.history.runIdFilter, null);
    assert.equal(state.history.runIdDraft, '');
    assert.equal(state.history.kindFilter, null);
    assert.ok(state.history.queryEpoch > epochBeforeReset);
  });

  it('prevents overlapping polls while a history query is already in flight', () => {
    let state = createConnectedActivityState();
    const first = beginHistoryQuery(state, 'initial');
    assert.ok(first.ticket !== null);
    state = first.state;
    assert.equal(state.history.inFlight, true);

    const overlappingPoll = beginHistoryQuery(state, 'poll', false);
    assert.equal(overlappingPoll.ticket, null);
    assert.equal(overlappingPoll.state.history.requestSeq, first.ticket.seq);

    const overlappingMore = beginHistoryQuery(state, 'more', false);
    assert.equal(overlappingMore.ticket, null);
  });

  it('drains anchored next_cursor first then queries fresh after_seq = completedHighWaterSeq without skipping unconsumed pages on poll', () => {
    let state = createConnectedActivityState();

    // 1. Initial query starts at afterSeq = 0, cursor = null
    const q1 = beginHistoryQuery(state, 'initial');
    assert.ok(q1.ticket !== null);
    assert.deepEqual(toActivityHistoryIpcArgs(q1.ticket), {
      bindingId: 'bind-alpha',
      afterSeq: 0,
      cursor: null,
      limit: 20,
      runId: null,
      kind: null,
    });
    state = q1.state;

    // Page 1 returns events [1, 2], has_more = true, next_cursor = 'cur-seq-2', high_water_mark_seq = 4
    state = applyHistoryPageSuccess(state, q1.ticket, {
      binding_id: 'bind-alpha',
      events: [makeEvent(1), makeEvent(2)],
      next_cursor: 'cur-seq-2',
      has_more: true,
      high_water_mark_seq: 4,
      after_seq: 0,
    });
    assert.equal(state.history.events.length, 2);
    assert.equal(state.history.hasMore, true);
    assert.equal(state.history.nextCursor, 'cur-seq-2');
    assert.equal(state.history.highWaterMarkSeq, 4);
    // Crucial: completedHighWaterSeq MUST remain 0 while anchored pages 3..4 are still unconsumed!
    assert.equal(state.history.completedHighWaterSeq, 0);

    // 2. A background poll (or More) while `nextCursor !== null` MUST drain `cur-seq-2` (afterSeq = null),
    //    and MUST NOT skip events 3..4 by sending afterSeq = 4!
    const pollWhileAnchored = beginHistoryQuery(state, 'poll');
    assert.ok(pollWhileAnchored.ticket !== null);
    assert.equal(pollWhileAnchored.ticket.cursor, 'cur-seq-2');
    assert.equal(pollWhileAnchored.ticket.afterSeq, null);
    state = pollWhileAnchored.state;

    // Page 2 drains the remaining anchored events [3, 4] with has_more = false, next_cursor = null
    state = applyHistoryPageSuccess(state, pollWhileAnchored.ticket, {
      binding_id: 'bind-alpha',
      events: [makeEvent(3), makeEvent(4)],
      next_cursor: null,
      has_more: false,
      high_water_mark_seq: 4,
      after_seq: 2,
    });
    assert.deepEqual(
      state.history.events.map((e) => e.seq),
      [1, 2, 3, 4]
    );
    assert.equal(state.history.hasMore, false);
    assert.equal(state.history.nextCursor, null);
    assert.equal(state.history.highWaterMarkSeq, 4);
    assert.equal(state.history.completedHighWaterSeq, 4);

    // 3. Now that the anchored window is completely drained, the next poll/refresh queries
    //    fresh events with afterSeq = completedHighWaterSeq (4) and cursor = null!
    const freshTailPoll = beginHistoryQuery(state, 'poll');
    assert.ok(freshTailPoll.ticket !== null);
    assert.equal(freshTailPoll.ticket.cursor, null);
    assert.equal(freshTailPoll.ticket.afterSeq, 4);
  });

  it('advances high-water mark and completedHighWaterSeq accurately on empty filtered pages', () => {
    let state = createConnectedActivityState();
    state = commitHistoryFilters(state, 'run-filtered', 'worker_completed');

    const q1 = beginHistoryQuery(state, 'initial');
    assert.ok(q1.ticket !== null);
    assert.equal(q1.ticket.runId, 'run-filtered');
    assert.equal(q1.ticket.kind, 'worker_completed');
    state = q1.state;

    // Server has 18 events on bind-alpha, but 0 match (run-filtered, worker_completed):
    // returns empty events, has_more = false, high_water_mark_seq = 18.
    state = applyHistoryPageSuccess(state, q1.ticket, {
      binding_id: 'bind-alpha',
      events: [],
      next_cursor: null,
      has_more: false,
      high_water_mark_seq: 18,
      after_seq: 0,
    });

    assert.equal(state.history.events.length, 0);
    assert.equal(state.history.highWaterMarkSeq, 18);
    assert.equal(state.history.completedHighWaterSeq, 18);
    assert.equal(state.history.hasMore, false);

    // Subsequent poll starts at afterSeq = 18 rather than re-scanning from 0
    const q2 = beginHistoryQuery(state, 'poll');
    assert.ok(q2.ticket !== null);
    assert.equal(q2.ticket.afterSeq, 18);
    assert.equal(q2.ticket.cursor, null);
  });

  it('caps retained history rows at MAX_RETAINED_HISTORY_EVENTS (200) and evicts stale selectedEventId', () => {
    let state = createConnectedActivityState();
    const q1 = beginHistoryQuery(state, 'initial');
    assert.ok(q1.ticket !== null);
    state = q1.state;

    const firstBatch = Array.from({ length: 150 }, (_, idx) => makeEvent(idx + 1));
    state = applyHistoryPageSuccess(state, q1.ticket, {
      binding_id: 'bind-alpha',
      events: firstBatch,
      next_cursor: 'cur-150',
      has_more: true,
      high_water_mark_seq: 250,
      after_seq: 0,
    });
    assert.equal(state.history.events.length, 150);

    // Select event #1 (which will be evicted once 250 events are loaded)
    state = selectHistoryEvent(state, 'evt-1');
    assert.equal(state.history.selectedEventId, 'evt-1');

    const q2 = beginHistoryQuery(state, 'more');
    assert.ok(q2.ticket !== null);
    state = q2.state;

    const secondBatch = Array.from({ length: 100 }, (_, idx) => makeEvent(idx + 151));
    state = applyHistoryPageSuccess(state, q2.ticket, {
      binding_id: 'bind-alpha',
      events: secondBatch,
      next_cursor: null,
      has_more: false,
      high_water_mark_seq: 250,
      after_seq: 150,
    });

    assert.equal(state.history.events.length, MAX_RETAINED_HISTORY_EVENTS);
    assert.equal(state.history.events[0]?.seq, 51);
    assert.equal(state.history.events[MAX_RETAINED_HISTORY_EVENTS - 1]?.seq, 250);
    assert.equal(
      state.history.selectedEventId,
      null,
      'selectedEventId for evicted row evt-1 must be cleared'
    );
  });

  it('never polls history on other pages, when run detail is open, when collapsed, or when document is hidden', () => {
    const state = createConnectedActivityState();
    assert.equal(shouldPollHistory(state, false), true);

    // Hidden document in background
    assert.equal(shouldPollHistory(state, true), false);
    assert.equal(beginHistoryQuery(state, 'poll', false, 20, true).ticket, null);

    // Collapsed History section
    const collapsed = setHistoryExpanded(state, false);
    assert.equal(shouldPollHistory(collapsed, false), false);
    assert.equal(beginHistoryQuery(collapsed, 'poll').ticket, null);

    // Run detail overlay open
    const withDetail = openRunDetail(state, 'bind-alpha', 'run-1').state;
    assert.equal(shouldPollHistory(withDetail, false), false);

    // Other navigation pages
    for (const page of ['chats', 'plans', 'tools', 'profiles', 'settings'] as const) {
      const onOtherPage = navigateToPage(state, page);
      assert.equal(
        shouldPollHistory(onOtherPage, false),
        false,
        `history must not poll on page ${page}`
      );
    }
  });

  it('surfaces honest bridge_disconnected in ordinary browser without Tauri or ?preview=1', async () => {
    await assert.rejects(
      async () => {
        await ipcDesktopActivityHistory({
          bindingId: 'bind-alpha',
          afterSeq: 0,
          cursor: null,
          limit: 20,
          runId: null,
          kind: null,
        });
      },
      (err: unknown) => {
        assert.ok(err instanceof Error);
        assert.match(err.message, /bridge_disconnected/);
        return true;
      }
    );
  });

  it('formats event kinds and timestamps safely', () => {
    const sampleKinds: EventKind[] = [
      'run_started',
      'question_asked',
      'worker_progress',
      'worker_completed',
      'worker_failed',
    ];
    for (const kind of sampleKinds) {
      assert.equal(isValidEventKind(kind), true);
      assert.ok(formatEventKindLabel(kind).length > 0);
      assert.ok(
        ['neutral', 'active', 'warn', 'danger', 'ok'].includes(resolveEventKindTone(kind))
      );
    }
    assert.equal(isValidEventKind('not_a_real_kind'), false);
    assert.equal(formatHistoryTimestamp(0), '--:--:--');
    assert.match(formatHistoryTimestamp(1730000000000), /^\d{2}:\d{2}:\d{2}$/);
  });
});
