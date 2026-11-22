import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CHAT_WORK_EXECUTION_BRIEF_GAP_MS,
  DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
  MAX_CHAT_WORK_EVENTS,
  MAX_CHAT_WORK_EXECUTIONS,
  MAX_CHAT_WORK_ITEMS,
  buildChatActivityEventViews,
  buildChatWorkBacklogGroups,
  buildChatWorkEventViews,
  buildChatWorkExecutionSummaries,
  buildChatWorkLivenessIndex,
  captureFirstVisibleMessageAnchor,
  compareWorkspaceMessageItems,
  computeAnchorScrollTop,
  createChatWorkExecutionContinuityStore,
  createMessageScrollState,
  createMessageScrollStore,
  distanceFromBottomPx,
  doesJobBelongToChat,
  doesMessageBelongToChat,
  doesObservationRequestMatchChat,
  evaluateChatWorkLiveness,
  filterJobsForChat,
  filterMessagesForChat,
  filterObservationRequestsForActivity,
  formatSidebarLifecycleWaitingLabel,
  formatSidebarWaitingCounter,
  formatSidebarWorkingCounter,
  getActiveChatScrollState,
  getChatScrollState,
  handleMessageViewportScroll,
  isNearBottom,
  jumpToLatestMessages,
  measureItemsFromContentOffsets,
  orderWorkspaceMessageItems,
  readWorkspaceChatWork,
  reconcileRefreshChatWorkPresenceEntries,
  resolveChatWorkExecutionDurationLabel,
  resolveChatWorkExecutionFooterState,
  resolveChatWorkExecutionFootersWithContinuity,
  resolveMessageScrollChatKey,
  resolveMessageViewportResize,
  resolveSidebarChatStatus,
  resolveVerifiedChatJobIds,
  selectWorkspaceChatWork,
  setChatScrollState,
  shouldRetainPreviousChatWorkPresence,
  summarizeChatWorkLiveness,
  switchMessageScrollChat,
  syncChatWorkEventScroll,
  syncMessageScrollItems,
} from './message-scroll.ts';
import type {
  ChatWorkEvent,
  ChatWorkExecution,
  ChatWorkExecutionSummaryView,
  ChatWorkItem,
  ChatWorkPresence,
  ContentOffsetMessageItem,
  MessageViewportMetrics,
} from './message-scroll.ts';
import type {
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
} from './observation/types.ts';
import {
  MAX_WORKSPACE_MESSAGES_WINDOW,
  createSafeWorkspaceMessageItem,
  normalizeWorkspaceChatSummary,
  normalizeWorkspaceJobHistory,
  normalizeWorkspaceNativeSample,
  projectSafeWorkspaceMessages,
  validateWorkspaceChatIdentity,
} from './workspace-contract.ts';
import type {
  WorkspaceChatIdentity,
  WorkspaceChatSummary,
  WorkspaceChatWork,
  WorkspaceMessageItem,
  WorkspaceNativeSample,
  WorkspaceSafeMessageSourceKind,
} from './workspace-contract.ts';

const UUID_ALPHA = '0196ed00-0000-7000-8000-000000000101';
const UUID_BETA = '0196ed00-0000-7000-8000-000000000202';

function makeIdentity(
  bindingId: string,
  threadUuid: string,
  projectScope = '/home/dev/repos/threadlane',
  sourceId = 'default',
): WorkspaceChatIdentity {
  return validateWorkspaceChatIdentity({
    source_id: sourceId,
    binding_id: bindingId,
    thread_uuid: threadUuid,
    canonical_project_scope: projectScope,
  });
}

function makeChatSummary(
  identity: WorkspaceChatIdentity,
  threadLabel = 'Alpha Thread',
): WorkspaceChatSummary {
  return normalizeWorkspaceChatSummary({
    identity,
    thread_label: threadLabel,
    project_label: 'threadlane',
    availability: 'available',
    activity: 'idle',
    configured_selection: {
      selected_model: 'gpt-6-astra',
      selected_effort: 'high',
      evidence: 'verified_configuration',
      counts_as_runtime_telemetry: false,
    },
    active_job_count: 0,
    attention_count: 0,
    updated_at_ms: 10_000,
  });
}

function makeMessage(options: {
  recordId: string;
  seq: number;
  recordedAtMs?: number | null;
  subIndex?: number;
  kind?: WorkspaceSafeMessageSourceKind;
  summary?: string;
  identity?: WorkspaceChatIdentity;
  sourceId?: string;
}): WorkspaceMessageItem {
  const id = options.identity;
  return createSafeWorkspaceMessageItem({
    source_kind: options.kind ?? 'task_result_summary',
    source_record_id: options.recordId,
    source_order: {
      recorded_at_ms: options.recordedAtMs === undefined ? 1_000 + options.seq : options.recordedAtMs,
      source_seq: options.seq,
      sub_index: options.subIndex ?? 0,
    },
    scope: id
      ? {
          source_id: id.source_id,
          binding_id: id.binding_id,
          thread_uuid: id.thread_uuid,
          canonical_project_scope: id.canonical_project_scope,
          chat_identity: id,
          job_id: 'job_01',
        }
      : {
          source_id: options.sourceId ?? 'default',
          job_id: 'job_01',
        },
    actor_role: 'coder',
    raw_summary: options.summary ?? `Summary for ${options.recordId}`,
  });
}

function buildSimulatedLayout(
  items: readonly { item_id: string }[],
  heightsById: ReadonlyMap<string, number>,
  defaultHeightPx = 60,
  gapPx = 8,
  topPaddingPx = 12,
  bottomPaddingPx = 12,
): { layoutItems: ContentOffsetMessageItem[]; scrollHeight: number } {
  const layoutItems: ContentOffsetMessageItem[] = [];
  let cursorY = topPaddingPx;
  for (let idx = 0; idx < items.length; idx += 1) {
    const item = items[idx]!;
    const heightPx = heightsById.get(item.item_id) ?? defaultHeightPx;
    layoutItems.push({
      itemId: item.item_id,
      offsetTopPx: cursorY,
      heightPx,
    });
    cursorY += heightPx;
    if (idx < items.length - 1) {
      cursorY += gapPx;
    }
  }
  const scrollHeight = items.length === 0 ? 0 : cursorY + bottomPaddingPx;
  return { layoutItems, scrollHeight };
}

test('orders messages deterministically by contract order, deduplicates stable item_id, and appends newer records at bottom', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA);
  const laterQuestion = makeMessage({
    recordId: 'q_02',
    seq: 20,
    recordedAtMs: 5_000,
    subIndex: 1,
    kind: 'question_answer',
    summary: 'Cevap: strict ayrımla ilerleyelim.',
    identity: idAlpha,
  });
  const earlierPrompt = makeMessage({
    recordId: 'q_02',
    seq: 20,
    recordedAtMs: 5_000,
    subIndex: 0,
    kind: 'question_prompt',
    summary: 'Soru: şema ayrımı nasıl olmalı?',
    identity: idAlpha,
  });
  const nullTimestampPlan = makeMessage({
    recordId: 'plan_legacy',
    seq: 99,
    recordedAtMs: null,
    subIndex: 0,
    kind: 'plan_summary',
    summary: 'Legacy plan without timestamp',
    identity: idAlpha,
  });
  const firstBrief = makeMessage({
    recordId: 'brief_01',
    seq: 1,
    recordedAtMs: 1_000,
    kind: 'work_brief_summary',
    summary: 'Initial brief draft',
    identity: idAlpha,
  });
  const updatedFirstBrief: WorkspaceMessageItem = {
    ...firstBrief,
    summary: 'Initial brief finalized',
  };

  const ordered = orderWorkspaceMessageItems([
    laterQuestion,
    firstBrief,
    earlierPrompt,
    nullTimestampPlan,
    updatedFirstBrief,
  ]);

  assert.equal(ordered.length, 4);
  assert.deepEqual(
    ordered.map((item) => item.source_record_id),
    ['plan_legacy', 'brief_01', 'q_02', 'q_02'],
  );
  assert.equal(ordered[1]!.summary, 'Initial brief finalized');
  assert.equal(ordered[2]!.source_kind, 'question_prompt');
  assert.equal(ordered[3]!.source_kind, 'question_answer');

  const appendedResult = makeMessage({
    recordId: 'res_03',
    seq: 21,
    recordedAtMs: 6_000,
    kind: 'job_result_summary',
    summary: 'Final job result appended at bottom',
    identity: idAlpha,
  });
  const afterAppend = orderWorkspaceMessageItems([...ordered, appendedResult]);
  assert.equal(afterAppend.at(-1)!.item_id, appendedResult.item_id);
  assert.ok(compareWorkspaceMessageItems(ordered.at(-1)!, appendedResult) < 0);
});

test('follows bottom on initial load, async first batch, and near-bottom updates without surfacing new-events', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA);
  let state = createMessageScrollState(resolveMessageScrollChatKey(idAlpha));

  // Empty loading pass before records arrive.
  const emptySync = syncMessageScrollItems(state, []);
  state = emptySync.state;
  assert.equal(state.mode, 'follow');
  assert.equal(state.pendingNewCount, 0);
  assert.equal(emptySync.action.kind, 'none');

  // Initial batch arrives: auto-scrolls to bottom and marks all items seen.
  const initialBatch = Array.from({ length: 8 }, (_, idx) =>
    makeMessage({ recordId: `rec_${idx}`, seq: idx + 1, identity: idAlpha }),
  );
  const firstSync = syncMessageScrollItems(state, initialBatch);
  state = firstSync.state;
  assert.equal(firstSync.action.kind, 'scroll_to_bottom');
  assert.equal(state.mode, 'follow');
  assert.equal(state.pendingNewCount, 0);

  // User scrolls slightly up (within DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX = 32px of bottom).
  const { layoutItems, scrollHeight } = buildSimulatedLayout(firstSync.orderedItems, new Map());
  const clientHeight = 240;
  const nearBottomScrollTop = scrollHeight - clientHeight - (DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX - 4);
  const nearBottomMetrics: MessageViewportMetrics = {
    scrollTop: nearBottomScrollTop,
    clientHeight,
    scrollHeight,
  };
  assert.equal(isNearBottom(nearBottomMetrics), true);
  assert.equal(
    distanceFromBottomPx(nearBottomMetrics),
    DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX - 4,
  );

  state = handleMessageViewportScroll(
    state,
    nearBottomMetrics,
    measureItemsFromContentOffsets(layoutItems, nearBottomScrollTop),
  );
  assert.equal(state.mode, 'follow');
  assert.equal(state.anchor, null);

  // Appending two new messages while near bottom stays in follow mode with zero pending notifications.
  const nextBatch = [
    ...initialBatch,
    makeMessage({ recordId: 'rec_8', seq: 9, identity: idAlpha }),
    makeMessage({ recordId: 'rec_9', seq: 10, identity: idAlpha }),
  ];
  const nextSync = syncMessageScrollItems(state, nextBatch);
  assert.equal(nextSync.action.kind, 'scroll_to_bottom');
  assert.equal(nextSync.state.mode, 'follow');
  assert.equal(nextSync.state.pendingNewCount, 0);
});

test('preserves first visible message anchor and exact pixel offset when reading older messages during appends and content updates', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA);
  const initialItems = Array.from({ length: 15 }, (_, idx) =>
    makeMessage({
      recordId: `msg_${idx}`,
      seq: idx + 1,
      summary: `Original summary ${idx}`,
      identity: idAlpha,
    }),
  );

  let state = syncMessageScrollItems(
    createMessageScrollState(resolveMessageScrollChatKey(idAlpha)),
    initialItems,
  ).state;

  const heights = new Map<string, number>();
  const initialLayout = buildSimulatedLayout(initialItems, heights, 60, 8, 12, 12);
  const clientHeight = 260;
  // Item 0: 12..72, Item 1: 80..140, Item 2: 148..208, Item 3: 216..276
  // At scrollTop = 234, Item 3 starts at 216 - 234 = -18px and ends at +42px (first visible item!).
  const readingScrollTop = 234;
  const readingMetrics: MessageViewportMetrics = {
    scrollTop: readingScrollTop,
    clientHeight,
    scrollHeight: initialLayout.scrollHeight,
  };
  assert.equal(isNearBottom(readingMetrics), false);

  const measuredBefore = measureItemsFromContentOffsets(
    initialLayout.layoutItems,
    readingScrollTop,
  );
  state = handleMessageViewportScroll(state, readingMetrics, measuredBefore);

  assert.equal(state.mode, 'reading');
  assert.deepEqual(state.anchor, {
    itemId: initialItems[3]!.item_id,
    offsetPx: -18,
  });
  assert.equal(state.pendingNewCount, 0);

  // Case 1: An older item ABOVE the anchor (`msg_1`) updates its summary and expands by +44px.
  // Because `item_id` is unchanged, `pendingNewCount` MUST stay 0, and `scrollTop` shifts +44px
  // so `msg_3` stays at exact -18px relative to viewport top.
  const expandedItems = initialItems.map((item, idx) =>
    idx === 1
      ? {
          ...item,
          summary: 'Updated Turkish summary: önceki görev tamamlandı ve doğrulandı.',
          completeness: {
            ...item.completeness,
            summary_truncated: true,
          },
        }
      : item,
  );
  heights.set(initialItems[1]!.item_id, 104); // +44px taller

  const updateSync = syncMessageScrollItems(state, expandedItems);
  state = updateSync.state;
  assert.equal(state.pendingNewCount, 0);
  assert.equal(updateSync.action.kind, 'preserve_anchor');
  if (updateSync.action.kind === 'preserve_anchor') {
    assert.equal(updateSync.action.anchor.itemId, initialItems[3]!.item_id);
    assert.equal(updateSync.action.anchor.offsetPx, -18);
  }

  const expandedLayout = buildSimulatedLayout(updateSync.orderedItems, heights, 60, 8, 12, 12);
  const measuredAfterExpand = measureItemsFromContentOffsets(
    expandedLayout.layoutItems,
    readingScrollTop,
  );
  const anchorMeasurementAfterExpand = measuredAfterExpand.find(
    (entry) => entry.itemId === initialItems[3]!.item_id,
  )!;
  assert.equal(anchorMeasurementAfterExpand.relativeTopPx, 26); // moved down +44px before compensation

  const compensatedScrollTop = computeAnchorScrollTop(
    {
      scrollTop: readingScrollTop,
      clientHeight,
      scrollHeight: expandedLayout.scrollHeight,
    },
    anchorMeasurementAfterExpand.relativeTopPx,
    state.anchor!.offsetPx,
  );
  assert.equal(compensatedScrollTop, readingScrollTop + 44);
  const verifiedAfterCompensation = measureItemsFromContentOffsets(
    expandedLayout.layoutItems,
    compensatedScrollTop,
  ).find((entry) => entry.itemId === initialItems[3]!.item_id)!;
  assert.equal(verifiedAfterCompensation.relativeTopPx, -18);

  // Case 2: Two genuinely new items arrive at the bottom while reading older entries.
  const newMsgA = makeMessage({ recordId: 'msg_15', seq: 16, identity: idAlpha });
  const newMsgB = makeMessage({ recordId: 'msg_16', seq: 17, identity: idAlpha });
  const withNewItems = [...expandedItems, newMsgA, newMsgB];
  const appendSync = syncMessageScrollItems(state, withNewItems);
  state = appendSync.state;

  assert.equal(state.mode, 'reading');
  assert.equal(state.pendingNewCount, 2);
  assert.equal(appendSync.action.kind, 'preserve_anchor');

  // Polling the exact same snapshot or updating an already-queued unseen message does NOT double-count.
  const updatedUnseenItems = withNewItems.map((item) =>
    item.item_id === newMsgA.item_id ? { ...item, summary: 'Updated unseen summary' } : item,
  );
  const pollAgainSync = syncMessageScrollItems(state, updatedUnseenItems);
  state = pollAgainSync.state;
  assert.equal(state.pendingNewCount, 2);

  // Explicit user click on the single controlled `new-events` button jumps to bottom and resets count.
  const jumped = jumpToLatestMessages(state);
  state = jumped.state;
  assert.equal(jumped.action.kind, 'scroll_to_bottom');
  assert.equal(state.mode, 'follow');
  assert.equal(state.anchor, null);
  assert.equal(state.pendingNewCount, 0);
});

test('captures positive pixel offset cleanly when scrollTop lands inside inter-item gap', () => {
  const items: ContentOffsetMessageItem[] = [
    { itemId: 'm0', offsetTopPx: 12, heightPx: 60 }, // 12..72
    { itemId: 'm1', offsetTopPx: 82, heightPx: 60 }, // 82..142 (10px gap: 72..82)
    { itemId: 'm2', offsetTopPx: 152, heightPx: 60 },
  ];
  // At scrollTop = 76, m0 is completely above viewport (-64..-4), while m1 starts +6px below top edge.
  const measured = measureItemsFromContentOffsets(items, 76);
  const anchor = captureFirstVisibleMessageAnchor(measured);
  assert.deepEqual(anchor, {
    itemId: 'm1',
    offsetPx: 6,
  });
});

test('prevents viewport jumps when the 1,000-message window trims older head entries', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA);
  const rawInputs = Array.from({ length: MAX_WORKSPACE_MESSAGES_WINDOW }, (_, seq) => ({
    source_kind: 'journal_summary' as const,
    source_record_id: `j_${seq}`,
    source_order: {
      recorded_at_ms: 10_000 + seq,
      source_seq: seq,
      sub_index: 0,
    },
    scope: {
      source_id: idAlpha.source_id,
      binding_id: idAlpha.binding_id,
      thread_uuid: idAlpha.thread_uuid,
      canonical_project_scope: idAlpha.canonical_project_scope,
    },
    raw_summary: `Entry ${seq}`,
  }));
  const fullWindow = projectSafeWorkspaceMessages(rawInputs);
  assert.equal(fullWindow.items.length, 1_000);

  let state = syncMessageScrollItems(
    createMessageScrollState(resolveMessageScrollChatKey(idAlpha)),
    fullWindow.items,
  ).state;

  const layoutBefore = buildSimulatedLayout(fullWindow.items, new Map(), 50, 6, 10, 10);
  // Each item + gap is 56px. Scroll to around item 10 (`10 + 10 * 56 = 570`).
  // At scrollTop = 582, item 10 starts at 570 - 582 = -12px.
  const scrollTopBefore = 582;
  state = handleMessageViewportScroll(
    state,
    {
      scrollTop: scrollTopBefore,
      clientHeight: 300,
      scrollHeight: layoutBefore.scrollHeight,
    },
    measureItemsFromContentOffsets(layoutBefore.layoutItems, scrollTopBefore),
  );
  assert.equal(state.anchor?.itemId, fullWindow.items[10]!.item_id);
  assert.equal(state.anchor?.offsetPx, -12);

  // Append 4 new entries -> `projectSafeWorkspaceMessages` trims `j_0..j_3` (4 * 56px = 224px removed from head!).
  const shiftedInputs = [
    ...rawInputs,
    ...Array.from({ length: 4 }, (_, idx) => {
      const seq = MAX_WORKSPACE_MESSAGES_WINDOW + idx;
      return {
        source_kind: 'journal_summary' as const,
        source_record_id: `j_${seq}`,
        source_order: {
          recorded_at_ms: 10_000 + seq,
          source_seq: seq,
          sub_index: 0,
        },
        scope: {
          source_id: idAlpha.source_id,
          binding_id: idAlpha.binding_id,
          thread_uuid: idAlpha.thread_uuid,
          canonical_project_scope: idAlpha.canonical_project_scope,
        },
        raw_summary: `Entry ${seq}`,
      };
    }),
  ];
  const trimmedWindow = projectSafeWorkspaceMessages(shiftedInputs);
  assert.equal(trimmedWindow.items.length, 1_000);
  assert.equal(trimmedWindow.omitted_count, 4);

  const trimSync = syncMessageScrollItems(state, trimmedWindow.items);
  state = trimSync.state;
  assert.equal(state.pendingNewCount, 4);
  assert.equal(trimSync.action.kind, 'preserve_anchor');
  if (trimSync.action.kind === 'preserve_anchor') {
    assert.equal(trimSync.action.anchorWasTrimmed, false);
    assert.equal(trimSync.action.anchor.itemId, fullWindow.items[10]!.item_id);
    assert.equal(trimSync.action.anchor.offsetPx, -12);
  }

  const layoutAfterTrim = buildSimulatedLayout(trimmedWindow.items, new Map(), 50, 6, 10, 10);
  const measuredAtStaleScroll = measureItemsFromContentOffsets(
    layoutAfterTrim.layoutItems,
    scrollTopBefore,
  );
  const anchorAtStaleScroll = measuredAtStaleScroll.find(
    (entry) => entry.itemId === fullWindow.items[10]!.item_id,
  )!;
  const adjustedScrollTop = computeAnchorScrollTop(
    {
      scrollTop: scrollTopBefore,
      clientHeight: 300,
      scrollHeight: layoutAfterTrim.scrollHeight,
    },
    anchorAtStaleScroll.relativeTopPx,
    state.anchor!.offsetPx,
  );
  assert.equal(adjustedScrollTop, scrollTopBefore - 4 * 56);

  const verifiedAfterTrim = measureItemsFromContentOffsets(
    layoutAfterTrim.layoutItems,
    adjustedScrollTop,
  ).find((entry) => entry.itemId === fullWindow.items[10]!.item_id)!;
  assert.equal(verifiedAfterTrim.relativeTopPx, -12);

  // Subcase B: Anchor itself is evicted by head-trimming (user was reading `j_5`, and `j_4..j_8` get trimmed).
  const topScrollTop = 78; // item `j_5` (index 1 in trimmedWindow: 10 + 56 = 66, relativeTop = -12)
  state = handleMessageViewportScroll(
    state,
    {
      scrollTop: topScrollTop,
      clientHeight: 300,
      scrollHeight: layoutAfterTrim.scrollHeight,
    },
    measureItemsFromContentOffsets(layoutAfterTrim.layoutItems, topScrollTop),
  );
  assert.equal(state.anchor?.itemId, trimmedWindow.items[1]!.item_id); // `j_5`

  const furtherShiftedWindow = projectSafeWorkspaceMessages([
    ...shiftedInputs,
    ...Array.from({ length: 5 }, (_, idx) => {
      const seq = MAX_WORKSPACE_MESSAGES_WINDOW + 4 + idx;
      return {
        source_kind: 'journal_summary' as const,
        source_record_id: `j_${seq}`,
        source_order: {
          recorded_at_ms: 10_000 + seq,
          source_seq: seq,
          sub_index: 0,
        },
        scope: {
          source_id: idAlpha.source_id,
          binding_id: idAlpha.binding_id,
          thread_uuid: idAlpha.thread_uuid,
          canonical_project_scope: idAlpha.canonical_project_scope,
        },
        raw_summary: `Entry ${seq}`,
      };
    }),
  ]);
  const evictedSync = syncMessageScrollItems(state, furtherShiftedWindow.items);
  assert.equal(evictedSync.state.mode, 'reading');
  assert.equal(evictedSync.action.kind, 'preserve_anchor');
  if (evictedSync.action.kind === 'preserve_anchor') {
    assert.equal(evictedSync.action.anchorWasTrimmed, true);
    assert.equal(evictedSync.action.anchor.itemId, furtherShiftedWindow.items[0]!.item_id);
    assert.equal(evictedSync.action.anchor.offsetPx, 0);
  }
});

test('stabilizes viewport across container and item resizes in both follow and reading modes', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA);
  const items = Array.from({ length: 12 }, (_, idx) =>
    makeMessage({ recordId: `r_${idx}`, seq: idx + 1, identity: idAlpha }),
  );
  let followState = syncMessageScrollItems(
    createMessageScrollState(resolveMessageScrollChatKey(idAlpha)),
    items,
  ).state;

  const baseLayout = buildSimulatedLayout(items, new Map(), 60, 8, 12, 12);
  // Resize in follow mode: container shrinks height from 320px to 200px -> stays pinned to new maxScrollTop.
  const followResize = resolveMessageViewportResize(
    followState,
    {
      scrollTop: baseLayout.scrollHeight - 320,
      clientHeight: 200,
      scrollHeight: baseLayout.scrollHeight,
    },
    measureItemsFromContentOffsets(baseLayout.layoutItems, baseLayout.scrollHeight - 320),
  );
  followState = followResize.state;
  assert.deepEqual(followResize.action, {
    kind: 'scroll_to_bottom',
    targetScrollTop: baseLayout.scrollHeight - 200,
  });

  // Switch to reading mode at item 4 with -22px offset.
  const item4OffsetTop = baseLayout.layoutItems[4]!.offsetTopPx;
  const initialReadingScrollTop = item4OffsetTop + 22;
  let readingState = handleMessageViewportScroll(
    followState,
    {
      scrollTop: initialReadingScrollTop,
      clientHeight: 220,
      scrollHeight: baseLayout.scrollHeight,
    },
    measureItemsFromContentOffsets(baseLayout.layoutItems, initialReadingScrollTop),
  );
  assert.equal(readingState.mode, 'reading');
  assert.deepEqual(readingState.anchor, {
    itemId: items[4]!.item_id,
    offsetPx: -22,
  });

  // Narrowing viewport wraps text, making every item 84px tall instead of 60px (+24px each).
  const wrappedLayout = buildSimulatedLayout(items, new Map(), 84, 8, 12, 12);
  const readingResize = resolveMessageViewportResize(
    readingState,
    {
      scrollTop: initialReadingScrollTop,
      clientHeight: 260,
      scrollHeight: wrappedLayout.scrollHeight,
    },
    measureItemsFromContentOffsets(wrappedLayout.layoutItems, initialReadingScrollTop),
  );
  readingState = readingResize.state;
  assert.equal(readingResize.action.kind, 'restore_anchor');
  if (readingResize.action.kind === 'restore_anchor') {
    const afterWrapped = measureItemsFromContentOffsets(
      wrappedLayout.layoutItems,
      readingResize.action.targetScrollTop,
    ).find((entry) => entry.itemId === items[4]!.item_id)!;
    assert.equal(afterWrapped.relativeTopPx, -22);
  }
  assert.equal(readingState.anchor?.offsetPx, -22);
});

test('isolates scroll state, anchors, and pending new-events strictly per canonical chat identity', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA, '/home/dev/repos/alpha');
  const idBeta = makeIdentity('bnd_beta', UUID_BETA, '/home/dev/repos/beta');

  const chatAlpha = makeChatSummary(idAlpha, 'Alpha Original Title');
  const chatAlphaRenamed = makeChatSummary(idAlpha, 'Alpha Renamed Title');
  const chatBeta = makeChatSummary(idBeta, 'Beta Chat');

  // Mutable display title must never alter canonical chat scroll key.
  assert.equal(
    resolveMessageScrollChatKey(chatAlpha),
    resolveMessageScrollChatKey(chatAlphaRenamed),
  );
  assert.notEqual(
    resolveMessageScrollChatKey(chatAlpha),
    resolveMessageScrollChatKey(chatBeta),
  );

  let store = createMessageScrollStore(chatAlpha);
  const alphaInitial = Array.from({ length: 10 }, (_, idx) =>
    makeMessage({ recordId: `alpha_${idx}`, seq: idx + 1, identity: idAlpha }),
  );

  let alphaState = syncMessageScrollItems(getActiveChatScrollState(store), alphaInitial).state;
  const alphaLayout = buildSimulatedLayout(alphaInitial, new Map(), 60, 8, 12, 12);
  alphaState = handleMessageViewportScroll(
    alphaState,
    { scrollTop: 160, clientHeight: 200, scrollHeight: alphaLayout.scrollHeight },
    measureItemsFromContentOffsets(alphaLayout.layoutItems, 160),
  );
  const alphaWithNew = [
    ...alphaInitial,
    makeMessage({ recordId: 'alpha_10', seq: 11, identity: idAlpha }),
    makeMessage({ recordId: 'alpha_11', seq: 12, identity: idAlpha }),
  ];
  alphaState = syncMessageScrollItems(alphaState, alphaWithNew).state;
  store = setChatScrollState(store, alphaState);

  assert.equal(getActiveChatScrollState(store).mode, 'reading');
  assert.equal(getActiveChatScrollState(store).pendingNewCount, 2);

  // Switch to Beta chat: clean follow state with zero leaked pending notifications or anchors.
  store = switchMessageScrollChat(store, chatBeta);
  const betaInitial = Array.from({ length: 5 }, (_, idx) =>
    makeMessage({ recordId: `beta_${idx}`, seq: idx + 1, identity: idBeta }),
  );
  const betaSync = syncMessageScrollItems(getActiveChatScrollState(store), betaInitial);
  store = setChatScrollState(store, betaSync.state);

  assert.equal(getActiveChatScrollState(store).mode, 'follow');
  assert.equal(getActiveChatScrollState(store).pendingNewCount, 0);
  assert.equal(getActiveChatScrollState(store).anchor, null);
  assert.equal(betaSync.action.kind, 'scroll_to_bottom');

  // Switch back to Alpha (even with renamed title): restores Alpha's exact reading anchor and 2 pending events.
  store = switchMessageScrollChat(store, chatAlphaRenamed);
  const restoredAlpha = getActiveChatScrollState(store);
  assert.equal(restoredAlpha.mode, 'reading');
  assert.equal(restoredAlpha.pendingNewCount, 2);
  assert.deepEqual(restoredAlpha.anchor, alphaState.anchor);
  assert.equal(getChatScrollState(store, chatBeta).mode, 'follow');
});

function makeObservationJob(overrides: Partial<ObservationJob> & { id: string }): ObservationJob {
  return {
    source_id: overrides.source_id ?? 'default',
    id: overrides.id,
    project_label: overrides.project_label ?? 'threadlane',
    project_path: overrides.project_path === undefined ? null : overrides.project_path,
    title: overrides.title ?? `Job ${overrides.id}`,
    thread_uuid: overrides.thread_uuid === undefined ? null : overrides.thread_uuid,
    thread_label: overrides.thread_label === undefined ? null : overrides.thread_label,
    binding_id: overrides.binding_id === undefined ? null : overrides.binding_id,
    plan_id: overrides.plan_id ?? null,
    plan_revision: overrides.plan_revision ?? null,
    state: overrides.state ?? 'running',
    approval_status: overrides.approval_status ?? 'approved',
    execution_status: overrides.execution_status ?? 'running',
    review_state: overrides.review_state ?? null,
    updated_at_ms: overrides.updated_at_ms ?? 10_000,
    active_call_count: overrides.active_call_count ?? 0,
    tasks: overrides.tasks ?? [],
    summary: overrides.summary ?? null,
  };
}

function makeObservationRequest(
  overrides: Partial<ObservationRequest> & { id: string },
): ObservationRequest {
  return {
    source_id: overrides.source_id ?? 'default',
    id: overrides.id,
    job_id: overrides.job_id === undefined ? null : overrides.job_id,
    parent_request_id: overrides.parent_request_id ?? null,
    title: overrides.title ?? `Request ${overrides.id}`,
    operation: overrides.operation ?? 'task.execute',
    project_label: overrides.project_label ?? 'threadlane',
    ...(overrides.project_path !== undefined ? { project_path: overrides.project_path } : {}),
    binding_id: overrides.binding_id === undefined ? null : overrides.binding_id,
    thread_uuid: overrides.thread_uuid === undefined ? null : overrides.thread_uuid,
    thread_label: overrides.thread_label === undefined ? null : overrides.thread_label,
    state: overrides.state ?? 'completed',
    started_at_ms: overrides.started_at_ms ?? 9_000,
    finished_at_ms: overrides.finished_at_ms ?? 9_500,
    updated_at_ms: overrides.updated_at_ms ?? 9_500,
    duration_ms: overrides.duration_ms ?? 500,
    actor: overrides.actor ?? {
      label: null,
      role: 'operator',
      source: 'codex',
      model: null,
      effort: null,
      evidence: 'unknown',
    },
    executor: overrides.executor ?? null,
    usage: overrides.usage ?? null,
    delivery: overrides.delivery ?? [],
    children: overrides.children ?? [],
    children_count: overrides.children_count ?? 0,
    summary: overrides.summary ?? null,
  };
}

test('filterJobsForChat requires exact four-part WorkspaceChatIdentity or verified non-conflicting known job mapping', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA, '/home/dev/repos/alpha');
  const idAlphaOtherWorktree = makeIdentity('bnd_alpha', UUID_ALPHA, '/home/dev/repos/alpha-wt');
  const idBeta = makeIdentity('bnd_beta', UUID_BETA, '/home/dev/repos/alpha');
  const chatAlpha = makeChatSummary(idAlpha, 'Alpha Thread');

  // Sole legacy job with matching source_id + binding_id ONLY (`!hasOtherChatJobs` trap) MUST be rejected.
  const legacyBindingOnlyJob = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_legacy_only',
      binding_id: 'bnd_alpha',
      chat_identity: null,
      title: 'Legacy binding-only job',
      state: 'completed',
      approval_status: 'approved',
      execution_status: 'completed',
    },
  });
  assert.equal(doesJobBelongToChat(legacyBindingOnlyJob, chatAlpha), false);
  assert.deepEqual(filterJobsForChat([legacyBindingOnlyJob], chatAlpha), []);

  // Global `chat = null` retains honest unscoped data without spilling into `chatAlpha`.
  assert.equal(filterJobsForChat([legacyBindingOnlyJob], null).length, 1);

  const exactAlphaJob = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_exact_alpha',
      binding_id: 'bnd_alpha',
      chat_identity: idAlpha,
      title: 'Exact Alpha job',
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
    },
  });
  const mismatchedWorktreeJob = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_other_worktree',
      binding_id: 'bnd_alpha',
      chat_identity: idAlphaOtherWorktree,
      title: 'Other worktree job',
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
    },
  });
  const mismatchedThreadJob = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_beta',
      binding_id: 'bnd_beta',
      chat_identity: idBeta,
      title: 'Beta job',
      state: 'completed',
      approval_status: 'approved',
      execution_status: 'completed',
    },
  });
  const conflictingRunJob = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_conflicting_run',
      binding_id: 'bnd_alpha',
      chat_identity: idAlpha,
      title: 'Conflicting run job',
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
    },
    runs: [
      {
        run_id: 'run_other_binding',
        binding_id: 'bnd_beta',
        status: 'running',
      },
    ],
  });

  // Incomplete history row (`chat_identity: null`) links ONLY when verified per-chat mapping proves its 4-part scope.
  const incompleteVerifiedHistory = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_obs_verified',
      binding_id: 'bnd_alpha',
      chat_identity: null,
      title: 'Incomplete history backed by verified observation job',
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
    },
  });
  const obsSnapshot: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 12_000,
    sources: [
      {
        id: 'default',
        label: 'Default',
        project_path: '/home/dev/repos/alpha',
        availability: 'available',
        message: null,
      },
    ],
    jobs: [
      makeObservationJob({
        id: 'job_obs_verified',
        source_id: 'default',
        binding_id: 'bnd_alpha',
        thread_uuid: UUID_ALPHA,
        project_path: '/home/dev/repos/alpha',
      }),
    ],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };

  const allHistories = [
    legacyBindingOnlyJob,
    exactAlphaJob,
    mismatchedWorktreeJob,
    mismatchedThreadJob,
    conflictingRunJob,
    incompleteVerifiedHistory,
  ];
  const verifiedIds = resolveVerifiedChatJobIds(chatAlpha, allHistories, obsSnapshot);
  assert.deepEqual(Array.from(verifiedIds).sort(), ['job_exact_alpha', 'job_obs_verified']);

  const filtered = filterJobsForChat(allHistories, chatAlpha, verifiedIds);
  assert.deepEqual(
    filtered.map((entry) => entry.job.job_id),
    ['job_exact_alpha', 'job_obs_verified'],
  );

  // Duplicate/colliding job_id on the same source where one record conflicts MUST reject both.
  const collidingBetaRecord = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_obs_verified',
      binding_id: 'bnd_beta',
      chat_identity: idBeta,
      title: 'Colliding job id on Beta',
      state: 'completed',
      approval_status: 'approved',
      execution_status: 'completed',
    },
  });
  const withCollision = [...allHistories, collidingBetaRecord];
  const verifiedAfterCollision = resolveVerifiedChatJobIds(chatAlpha, withCollision, obsSnapshot);
  assert.equal(verifiedAfterCollision.has('job_obs_verified'), false);
  assert.deepEqual(
    filterJobsForChat(withCollision, chatAlpha, verifiedIds).map((entry) => entry.job.job_id),
    ['job_exact_alpha'],
  );
});

test('filterMessagesForChat rejects partial scopes and links incomplete messages only via verified non-conflicting job ids', () => {
  const idAlpha = makeIdentity('bnd_alpha', UUID_ALPHA, '/home/dev/repos/alpha');
  const chatAlpha = makeChatSummary(idAlpha, 'Alpha Thread');

  const bindingOnlyMsg = createSafeWorkspaceMessageItem({
    source_kind: 'journal_summary',
    source_record_id: 'msg_binding_only',
    source_order: { recorded_at_ms: 1_000, source_seq: 1, sub_index: 0 },
    scope: {
      source_id: 'default',
      binding_id: 'bnd_alpha',
      job_id: 'job_unverified',
    },
    raw_summary: 'Ambiguous binding-only message',
  });
  const threadAndBindingNoProjectMsg = createSafeWorkspaceMessageItem({
    source_kind: 'journal_summary',
    source_record_id: 'msg_no_project',
    source_order: { recorded_at_ms: 1_001, source_seq: 2, sub_index: 0 },
    scope: {
      source_id: 'default',
      binding_id: 'bnd_alpha',
      thread_uuid: UUID_ALPHA,
      job_id: 'job_unverified',
    },
    raw_summary: 'Missing canonical_project_scope',
  });

  // Sole partial messages in window (`!hasOtherChatItems` trap) MUST NOT match a selected chat.
  assert.equal(doesMessageBelongToChat(bindingOnlyMsg, chatAlpha), false);
  assert.equal(doesMessageBelongToChat(threadAndBindingNoProjectMsg, chatAlpha), false);
  assert.deepEqual(
    filterMessagesForChat([bindingOnlyMsg, threadAndBindingNoProjectMsg], chatAlpha),
    [],
  );
  // Global `chat = null` retains unscoped messages honestly.
  assert.equal(filterMessagesForChat([bindingOnlyMsg], null).length, 1);

  const exactMsg = makeMessage({
    recordId: 'msg_exact',
    seq: 3,
    identity: idAlpha,
  });
  const verifiedIncompleteMsg = createSafeWorkspaceMessageItem({
    source_kind: 'task_result_summary',
    source_record_id: 'msg_verified_job',
    source_order: { recorded_at_ms: 1_004, source_seq: 4, sub_index: 0 },
    scope: {
      source_id: 'default',
      binding_id: 'bnd_alpha',
      job_id: 'job_verified_alpha',
    },
    raw_summary: 'Linked via verified per-chat job',
  });
  const knownJobButMismatchedThreadMsg = createSafeWorkspaceMessageItem({
    source_kind: 'task_result_summary',
    source_record_id: 'msg_conflict_thread',
    source_order: { recorded_at_ms: 1_005, source_seq: 5, sub_index: 0 },
    scope: {
      source_id: 'default',
      binding_id: 'bnd_alpha',
      thread_uuid: UUID_BETA,
      job_id: 'job_verified_alpha',
    },
    raw_summary: 'Known job id but mismatched thread_uuid',
  });

  const knownJobs = new Set<string>(['job_verified_alpha']);
  assert.equal(doesMessageBelongToChat(verifiedIncompleteMsg, chatAlpha, knownJobs), true);
  assert.equal(doesMessageBelongToChat(knownJobButMismatchedThreadMsg, chatAlpha, knownJobs), false);

  const nonConflictingBatch = [
    bindingOnlyMsg,
    threadAndBindingNoProjectMsg,
    exactMsg,
    verifiedIncompleteMsg,
  ];
  assert.deepEqual(
    filterMessagesForChat(nonConflictingBatch, chatAlpha, knownJobs).map(
      (item) => item.source_record_id,
    ),
    ['msg_exact', 'msg_verified_job'],
  );

  // If a sibling message on the same job_id carries a conflicting thread_uuid, `filterMessagesForChat` quarantines that job_id.
  assert.deepEqual(
    filterMessagesForChat(
      [...nonConflictingBatch, knownJobButMismatchedThreadMsg],
      chatAlpha,
      knownJobs,
    ).map((item) => item.source_record_id),
    ['msg_exact'],
  );
});

test('doesObservationRequestMatchChat and filterObservationRequestsForActivity enforce strict scope and intersect job filters', () => {
  const idAlphaWorktree = makeIdentity(
    'bnd_alpha',
    UUID_ALPHA,
    '/home/dev/repos/threadlane-wt-alpha',
  );
  const idBetaMain = makeIdentity('bnd_beta', UUID_BETA, '/home/dev/repos/threadlane');
  const chatAlpha = makeChatSummary(idAlphaWorktree, 'Shared Display Title');

  // Sole unscoped request with matching display labels ONLY (`!hasOtherChatRequests` + label trap) MUST NOT match.
  const soleUnscopedReq = makeObservationRequest({
    id: 'req_sole_unscoped',
    thread_label: 'Shared Display Title',
    project_label: 'threadlane',
    binding_id: null,
    thread_uuid: null,
    job_id: null,
  });
  assert.equal(doesObservationRequestMatchChat(soleUnscopedReq, chatAlpha), false);
  assert.deepEqual(
    filterObservationRequestsForActivity([soleUnscopedReq], { chat: chatAlpha }),
    [],
  );
  assert.equal(
    filterObservationRequestsForActivity([soleUnscopedReq], { chat: null }).length,
    1,
  );

  // Partial binding-only or thread-only requests without verified 4-part scope MUST NOT match.
  const bindingOnlyReq = makeObservationRequest({
    id: 'req_binding_only',
    binding_id: 'bnd_alpha',
    thread_uuid: null,
  });
  const threadOnlyReq = makeObservationRequest({
    id: 'req_thread_only',
    binding_id: null,
    thread_uuid: UUID_ALPHA,
  });

  // Verified worktree job history on `chatAlpha` (even though `ObservationSource.project_path` points to main repo).
  const alphaWorktreeHistory = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_alpha_wt',
      binding_id: 'bnd_alpha',
      chat_identity: idAlphaWorktree,
      title: 'Alpha worktree job',
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
    },
  });
  const betaHistory = normalizeWorkspaceJobHistory({
    job: {
      source_id: 'default',
      job_id: 'job_beta_main',
      binding_id: 'bnd_beta',
      chat_identity: idBetaMain,
      title: 'Beta main job',
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
    },
  });

  const multiProjectObs: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 20_000,
    sources: [
      {
        id: 'default',
        label: 'Default Source',
        project_path: '/home/dev/repos/threadlane',
        availability: 'available',
        message: null,
      },
    ],
    jobs: [
      makeObservationJob({
        id: 'job_beta_main',
        source_id: 'default',
        binding_id: 'bnd_beta',
        thread_uuid: UUID_BETA,
        project_path: '/home/dev/repos/threadlane',
      }),
    ],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };

  const alphaJobReq = makeObservationRequest({
    id: 'req_alpha_job',
    job_id: 'job_alpha_wt',
    binding_id: null,
    thread_uuid: null,
  });
  const alphaJobReqWithConflictingThread = makeObservationRequest({
    id: 'req_alpha_job_bad_thread',
    job_id: 'job_alpha_wt',
    binding_id: 'bnd_alpha',
    thread_uuid: UUID_BETA,
  });
  const betaJobReq = makeObservationRequest({
    id: 'req_beta_job',
    job_id: 'job_beta_main',
    binding_id: 'bnd_beta',
    thread_uuid: UUID_BETA,
  });

  const histories = [alphaWorktreeHistory, betaHistory];
  const verifiedAlphaJobs = resolveVerifiedChatJobIds(chatAlpha, histories, multiProjectObs);
  assert.deepEqual(Array.from(verifiedAlphaJobs), ['job_alpha_wt']);

  const allRequests = [
    soleUnscopedReq,
    bindingOnlyReq,
    threadOnlyReq,
    alphaJobReq,
    alphaJobReqWithConflictingThread,
    betaJobReq,
  ];

  // Unscoped Activity on selected `chatAlpha` returns ONLY `req_alpha_job`.
  assert.deepEqual(
    filterObservationRequestsForActivity(allRequests, {
      chat: chatAlpha,
      knownChatJobIds: verifiedAlphaJobs,
      observation: multiProjectObs,
      jobHistories: histories,
    }).map((req) => req.id),
    ['req_alpha_job'],
  );

  // Job-scoped Activity MUST intersect selected-chat scope, NEVER bypass it!
  assert.deepEqual(
    filterObservationRequestsForActivity(allRequests, {
      chat: chatAlpha,
      jobScope: { source_id: 'default', job_id: 'job_beta_main' },
      knownChatJobIds: verifiedAlphaJobs,
      observation: multiProjectObs,
      jobHistories: histories,
    }),
    [],
  );
  assert.deepEqual(
    filterObservationRequestsForActivity(allRequests, {
      chat: chatAlpha,
      jobScope: { source_id: 'default', job_id: 'job_alpha_wt' },
      knownChatJobIds: verifiedAlphaJobs,
      observation: multiProjectObs,
      jobHistories: histories,
    }).map((req) => req.id),
    ['req_alpha_job'],
  );

  // Global `chat = null` with `jobScope` retains that job's requests without spilling into `chatAlpha`.
  assert.deepEqual(
    filterObservationRequestsForActivity(allRequests, {
      chat: null,
      jobScope: { source_id: 'default', job_id: 'job_beta_main' },
    }).map((req) => req.id),
    ['req_beta_job'],
  );
});

// ---------------------------------------------------------------------------
// Minimal chat-work stream and backlog drawer contract
// ---------------------------------------------------------------------------

function makeChatWork(
  identity: WorkspaceChatIdentity,
  overrides: Partial<WorkspaceChatWork> = {},
): WorkspaceChatWork {
  return {
    identity,
    availability: 'available',
    items: [],
    events: [],
    worker_presence: [],
    bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
    ...overrides,
  } as unknown as WorkspaceChatWork;
}

function makeBacklogItem(overrides: Partial<ChatWorkItem> = {}): ChatWorkItem {
  return {
    item_id: 'blg_alpha',
    binding_id: 'bnd_alpha',
    revision: 1,
    title: 'Implement the approved slice',
    acceptance_condition: 'The approved slice is checked.',
    delivery_description: 'A small reviewed change.',
    kind: 'coworker',
    status: 'todo',
    blocked_reason: null,
    current_execution_ref: null,
    completion_note: null,
    completion_evidence: null,
    completed_at_ms: null,
    created_at_ms: 1_000,
    updated_at_ms: 1_000,
    ...overrides,
  } as ChatWorkItem;
}

function makeBacklogEvent(overrides: Partial<ChatWorkEvent> = {}): ChatWorkEvent {
  return {
    event_id: 'blg_alpha:1',
    sequence: 1,
    item_id: 'blg_alpha',
    revision: 1,
    action: 'created',
    recorded_at_ms: 1_000,
    title: 'Implement the approved slice',
    status: 'todo',
    execution_ref: null,
    outcome: null,
    note: null,
    ...overrides,
  } as ChatWorkEvent;
}

type ChatWorkPresenceFixtureOverrides = {
  readonly [Key in keyof ChatWorkPresence]?: ChatWorkPresence[Key] | null;
};

function makePresence(
  overrides: ChatWorkPresenceFixtureOverrides = {},
): ChatWorkPresence {
  const presence = {
    item_id: 'blg_alpha',
    execution_ref: 'exec-1',
    approval_id: 'approval-1',
    task_id: 'task-1',
    run_id: 'run-1',
    worker_id: 'worker-1',
    claim_id: 'claim-1',
    host_instance_id: 'host-1',
    invocation_id: 'invocation-1',
    sequence: 4,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_000,
    sample_age_ms: 100,
    max_sample_age_ms: 5_000,
    ...overrides,
  } as ChatWorkPresence;
  const sampledAtMs =
    typeof presence.sampled_at_ms === 'number' ? presence.sampled_at_ms : 0;
  const maxSampleAgeMs =
    typeof presence.max_sample_age_ms === 'number' ? presence.max_sample_age_ms : 0;
  return {
    ...presence,
    expires_at_ms:
      overrides.expires_at_ms === undefined
        ? sampledAtMs + maxSampleAgeMs
        : overrides.expires_at_ms,
  } as ChatWorkPresence;
}

type ChatWorkExecutionFixtureOverrides = {
  readonly [Key in keyof ChatWorkExecution]?: ChatWorkExecution[Key] | null;
};

function makeExecution(
  bindingId: string,
  overrides: ChatWorkExecutionFixtureOverrides = {},
): ChatWorkExecution {
  return {
    item_id: 'blg_alpha',
    execution_ref: 'exec-1',
    approval_id: 'approval-1',
    task_id: 'task-1',
    session_binding_id: bindingId,
    run_id: 'run-1',
    worker_id: 'worker-1',
    provider: 'opencode',
    requested_model: 'deepseek-v4.1-flash',
    observed_model: null,
    requested_effort: 'max',
    observed_effort: null,
    worker_status: 'running',
    started_at_ms: 9_000,
    finished_at_ms: null,
    duration_ms: null,
    outcome: null,
    acceptance: null,
    ...overrides,
  } as ChatWorkExecution;
}

function makeNativeSample(
  identity: WorkspaceChatIdentity,
  activity:
    | 'working'
    | 'waiting_for_approval'
    | 'waiting_for_input'
    | 'idle'
    | 'unknown',
  observedAtMs = 10_000,
  availability: 'available' | 'stale' | 'disconnected' | 'unavailable' = 'available',
): WorkspaceNativeSample {
  return normalizeWorkspaceNativeSample({
    chat_identity: identity,
    producer_epoch: 'epoch-test-1',
    backend_process_instance: null,
    sequence: 2,
    observed_at_ms: observedAtMs,
    last_success_at_ms: observedAtMs,
    availability,
    activity,
    connection: {
      state: 'connected',
      freshness: 'fresh',
      route: 'native-test',
      diagnostic: null,
    },
    capabilities: {
      exact_thread_read: 'supported',
      activity_detail: 'supported',
      configured_selection: 'supported',
      wakeup_delivery: 'supported',
      per_call_identity: 'supported',
    },
    configured_selection: {
      selected_model: 'gpt-6-astra',
      selected_effort: 'high',
      evidence: 'verified_configuration',
      counts_as_runtime_telemetry: false,
    },
  });
}

const CHAT_WORK_NOW_MS = 10_500;

test('chat_work selection uses exact canonical identity and never falls back to legacy jobs or messages', () => {
  const alphaIdentity = makeIdentity('bnd_alpha_chat_work', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_beta_chat_work', UUID_BETA);
  const betaWork = makeChatWork(betaIdentity, {
    items: [makeBacklogItem({ item_id: 'blg_beta' })],
  });

  const legacyShapedSnapshot = {
    schema_version: 1,
    generated_at_ms: CHAT_WORK_NOW_MS,
    chats: [{ identity: alphaIdentity, thread_label: 'Alpha' }],
    job_histories: [{ job: { job_id: 'job_legacy_alpha', title: 'Legacy job' } }],
    messages: { items: [{ item_id: 'msg_legacy_alpha' }] },
  };

  assert.deepEqual(readWorkspaceChatWork(legacyShapedSnapshot), []);
  const fallbackSelection = selectWorkspaceChatWork(
    readWorkspaceChatWork(legacyShapedSnapshot),
    alphaIdentity,
  );
  assert.equal(fallbackSelection.chatKey, resolveMessageScrollChatKey(alphaIdentity));
  assert.equal(fallbackSelection.availability, 'unavailable');
  assert.deepEqual(fallbackSelection.items, []);
  assert.deepEqual(fallbackSelection.events, []);
  assert.equal(fallbackSelection.unavailableReason, 'No backlog data is recorded for this chat yet.');

  const isolated = selectWorkspaceChatWork([betaWork], alphaIdentity);
  assert.equal(isolated.availability, 'unavailable');
  assert.deepEqual(isolated.items, []);

  const exact = selectWorkspaceChatWork([betaWork], betaIdentity);
  assert.equal(exact.availability, 'available');
  assert.deepEqual(exact.items.map((item) => item.item_id), ['blg_beta']);

  const legacyLiveness = evaluateChatWorkLiveness(
    fallbackSelection,
    makeNativeSample(alphaIdentity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(legacyLiveness.isWorking, false);
  assert.equal(legacyLiveness.waitingForHuman, false);
  assert.notEqual(
    resolveMessageScrollChatKey({ ...alphaIdentity, binding_id: '' }),
    resolveMessageScrollChatKey(alphaIdentity),
  );
});

test('chat_work events order by actual history sequence, keep stable scoped keys, and only record actual outcomes once', () => {
  const identity = makeIdentity('bnd_alpha_events', UUID_ALPHA);
  const work = makeChatWork(identity, {
    events: [
      makeBacklogEvent({
        event_id: 'blg_alpha:5',
        sequence: 15,
        revision: 5,
        action: 'completed',
        status: 'done',
        recorded_at_ms: 1_500,
        outcome: 'success',
      }),
      makeBacklogEvent({
        event_id: 'blg_alpha:2',
        sequence: 12,
        revision: 2,
        action: 'worker_reserved',
        status: 'doing',
        execution_ref: 'exec-1',
        recorded_at_ms: 1_200,
      }),
      makeBacklogEvent({
        event_id: 'blg_alpha:3',
        sequence: 13,
        revision: 3,
        action: 'worker_outcome_recorded',
        status: 'doing',
        execution_ref: 'exec-1',
        outcome: 'success',
        recorded_at_ms: 1_300,
        note: 'The Worker reported a successful result.',
      }),
      makeBacklogEvent({
        event_id: 'blg_alpha:2',
        sequence: 12,
        revision: 2,
        action: 'worker_reserved',
        status: 'doing',
        recorded_at_ms: 1_200,
      }),
      makeBacklogEvent({
        event_id: 'blg_alpha:4',
        sequence: 14,
        revision: 4,
        action: 'worker_outcome_recorded',
        status: 'doing',
        execution_ref: 'exec-1',
        outcome: 'failure',
        recorded_at_ms: 1_400,
      }),
    ],
  });

  const selection = selectWorkspaceChatWork([work], identity);
  const views = buildChatWorkEventViews(selection);
  assert.deepEqual(views.map((event) => event.sequence), [12, 13, 14, 15]);
  assert.deepEqual(
    views.map((event) => event.actionLabel),
    ['Preparing', 'Awaiting check', 'Failed', 'Done'],
  );
  assert.equal(views[0]!.title, 'Implement the approved slice');
  assert.equal(views[1]!.outcome, 'success');
  assert.equal(views[3]!.outcome, null);
  assert.equal(new Set(views.map((event) => event.key)).size, views.length);
  assert.ok(views.every((event) => event.key.startsWith(`${resolveMessageScrollChatKey(identity)}\u0000`)));
});

test('scoped planning requests update the same stream row without changing backlog order', () => {
  const identity = makeIdentity('bnd_planning', UUID_ALPHA);
  const chat = makeChatSummary(identity);
  const selection = selectWorkspaceChatWork(
    [makeChatWork(identity, {
      events: [
        makeBacklogEvent({ event_id: 'backlog-created', sequence: 1, recorded_at_ms: 1_000 }),
        makeBacklogEvent({ event_id: 'backlog-done', sequence: 2, recorded_at_ms: 3_000 }),
      ],
    })],
    identity,
  );
  const advisor = makeObservationRequest({
    id: 'advisor-completed',
    operation: 'coworker.planning.advisor',
    job_id: 'planning-job',
    binding_id: identity.binding_id,
    thread_uuid: identity.thread_uuid,
    started_at_ms: 1_500,
    finished_at_ms: 3_500,
    updated_at_ms: 3_500,
    duration_ms: 2_000,
  });
  const advisorStart: ObservationRequest = {
    ...advisor,
    state: 'unknown',
    finished_at_ms: null,
    updated_at_ms: 1_500,
    duration_ms: null,
  };
  const orchestrator = {
    ...advisor,
    id: 'orchestrator-completed',
    operation: 'coworker.planning.orchestrator',
    started_at_ms: 4_000,
    finished_at_ms: null,
    updated_at_ms: 4_500,
    duration_ms: null,
  };
  const observation: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 5_000,
    sources: [{
      id: identity.source_id,
      label: 'Threadlane',
      project_path: identity.canonical_project_scope,
      availability: 'available',
      message: null,
    }],
    jobs: [
      makeObservationJob({
        id: 'planning-job',
        binding_id: identity.binding_id,
        thread_uuid: identity.thread_uuid,
        project_path: identity.canonical_project_scope,
      }),
      makeObservationJob({
        id: 'other-project-job',
        binding_id: identity.binding_id,
        thread_uuid: identity.thread_uuid,
        project_path: '/home/dev/repos/other-worktree',
      }),
    ],
    requests: [
      orchestrator,
      advisorStart,
      advisor,
      advisorStart,
      { ...advisorStart, id: 'no-start', started_at_ms: null },
      {
        ...advisorStart,
        id: 'outcome-unknown',
        started_at_ms: 4_800,
        finished_at_ms: 4_900,
        updated_at_ms: 4_900,
      },
      { ...advisor, id: 'wrong-chat', thread_uuid: UUID_BETA },
      { ...advisor, id: 'wrong-project', job_id: 'other-project-job' },
      { ...advisor, id: 'wrong-source', source_id: 'other-source' },
      { ...advisor, id: 'unscoped', job_id: null, binding_id: null },
      { ...advisor, id: 'cli-bookkeeping', operation: 'flow.coworker-planning.start-command' },
    ],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };

  const views = buildChatActivityEventViews(selection, chat, { observation, job_histories: [] });
  assert.deepEqual(views.map((view) => view.eventId), [
    'backlog-created',
    'planning:advisor-completed',
    'backlog-done',
    'planning:orchestrator-completed',
    'planning:outcome-unknown',
  ]);
  const [completed, olderRecord, unknown] = views.filter((view) => 'planningRole' in view);
  assert.equal(completed?.status, 'completed');
  assert.equal(completed?.recordedAtMs, 3_500);
  assert.equal(completed?.durationLabel, '2s');
  assert.equal(olderRecord?.recordedAtMs, 4_500);
  assert.equal(olderRecord?.durationLabel, null);
  assert.equal(unknown?.actionLabel, 'Outcome unknown');
  assert.equal(unknown?.durationLabel, '100ms');

  const started = buildChatActivityEventViews(selection, chat, {
    observation: { ...observation, requests: [advisorStart] },
    job_histories: [],
  }).find((view) => 'planningRole' in view);
  assert.equal(started?.key, completed?.key);
  assert.equal(started?.status, 'started');
  assert.equal(started?.tone, 'neutral');
  assert.equal(started?.durationLabel, null);
  assert.deepEqual(
    views.filter((view) => !('planningRole' in view)),
    buildChatWorkEventViews(selection),
  );
});

test('jobless planning requests match exact canonical request.project_path on default source (project_path=null) while refusing mismatch, invalid, and conflicting scopes', () => {
  const alphaProject = '/home/dev/repos/alpha-app';
  const otherProject = '/home/dev/repos/other-app';
  const identity = makeIdentity('bnd_advisor_alpha', UUID_ALPHA, alphaProject);
  const chat = makeChatSummary(identity, 'Shared Chat Label');
  const selection = selectWorkspaceChatWork([makeChatWork(identity)], identity);

  const defaultSourceWithNullPath: ObservationSnapshot['sources'][number] = {
    id: identity.source_id,
    label: 'Threadlane local',
    project_path: null,
    availability: 'available',
    message: null,
  };
  const alphaJobHistory = normalizeWorkspaceJobHistory({
    job: {
      source_id: identity.source_id,
      job_id: 'job_alpha_verified',
      binding_id: identity.binding_id,
      chat_identity: identity,
      title: 'Alpha Job',
      state: 'completed',
      approval_status: 'approved',
      execution_status: 'completed',
    },
  });
  const otherProjectJob = makeObservationJob({
    id: 'job_other_project',
    source_id: identity.source_id,
    binding_id: identity.binding_id,
    thread_uuid: identity.thread_uuid,
    project_path: otherProject,
  });
  const alphaProjectJob = makeObservationJob({
    id: 'job_alpha_verified',
    source_id: identity.source_id,
    binding_id: identity.binding_id,
    thread_uuid: identity.thread_uuid,
    project_path: alphaProject,
  });

  const validAdvisorCompletion = makeObservationRequest({
    id: 'advisor-jobless-exact-scope',
    operation: 'coworker.planning.advisor',
    job_id: null,
    project_label: 'Unrelated Label',
    project_path: alphaProject,
    binding_id: identity.binding_id,
    thread_uuid: identity.thread_uuid,
    thread_label: 'Shared Chat Label',
    state: 'completed',
    started_at_ms: 2_000,
    finished_at_ms: 4_000,
    updated_at_ms: 4_000,
    duration_ms: 2_000,
  });

  const wrongProjectSameBindingThread = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-wrong-project',
    project_label: 'alpha-app',
    project_path: otherProject,
  });

  const wrongProjectEvenWithMatchingJob = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-wrong-project-with-alpha-job',
    job_id: 'job_alpha_verified',
    project_path: otherProject,
  });

  const matchingProjectConflictingJob = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-conflicting-job',
    job_id: 'job_other_project',
    project_path: alphaProject,
  });

  const wrongThreadSameBindingProject = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-wrong-thread',
    thread_uuid: UUID_BETA,
  });

  const wrongBindingSameThreadProject = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-wrong-binding',
    binding_id: 'bnd_other',
  });

  const threadOnlyMissingBinding = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-missing-binding',
    binding_id: null,
  });

  const wrongSourceSameFields = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-wrong-source',
    source_id: 'src_other',
  });

  const omittedScopeNoJobOnNullSource = makeObservationRequest({
    ...validAdvisorCompletion,
    id: 'advisor-omitted-scope-unverified',
    project_label: 'alpha-app',
    project_path: null,
  });

  const defaultObs: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 5_000,
    sources: [defaultSourceWithNullPath],
    jobs: [alphaProjectJob, otherProjectJob],
    requests: [
      validAdvisorCompletion,
      wrongProjectSameBindingThread,
      wrongProjectEvenWithMatchingJob,
      matchingProjectConflictingJob,
      wrongThreadSameBindingProject,
      wrongBindingSameThreadProject,
      threadOnlyMissingBinding,
      wrongSourceSameFields,
      omittedScopeNoJobOnNullSource,
    ],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };

  assert.equal(
    doesObservationRequestMatchChat(validAdvisorCompletion, chat, {
      observation: defaultObs,
      jobHistories: [alphaJobHistory],
      knownChatJobIds: new Set(['job_alpha_verified']),
    }),
    true,
  );
  assert.equal(
    doesObservationRequestMatchChat(wrongProjectSameBindingThread, chat, {
      observation: defaultObs,
      jobHistories: [alphaJobHistory],
    }),
    false,
  );
  assert.equal(
    doesObservationRequestMatchChat(wrongProjectEvenWithMatchingJob, chat, {
      observation: defaultObs,
      jobHistories: [alphaJobHistory],
      knownChatJobIds: new Set(['job_alpha_verified']),
    }),
    false,
  );
  assert.equal(
    doesObservationRequestMatchChat(matchingProjectConflictingJob, chat, {
      observation: defaultObs,
      jobHistories: [alphaJobHistory],
    }),
    false,
  );

  // Non-canonical explicit `request.project_path` must be refused even when source or job matches.
  const linkedMatchingSource: ObservationSnapshot = {
    ...defaultObs,
    sources: [{ ...defaultSourceWithNullPath, project_path: alphaProject }],
  };
  for (const invalidPath of [
    '',
    '   ',
    'relative/alpha-app',
    ' /home/dev/repos/alpha-app',
    '/home/dev/repos/alpha-app/',
    '/home/dev/repos/../alpha-app',
    '/home/dev//repos/alpha-app',
    '/tmp/sk-secret123456',
  ]) {
    const invalidScopeReq = makeObservationRequest({
      ...validAdvisorCompletion,
      id: `advisor-invalid-${invalidPath.length}`,
      job_id: 'job_alpha_verified',
      project_path: invalidPath,
    });
    assert.equal(
      doesObservationRequestMatchChat(invalidScopeReq, chat, {
        observation: linkedMatchingSource,
        jobHistories: [alphaJobHistory],
        knownChatJobIds: new Set(['job_alpha_verified']),
      }),
      false,
      `expected invalid request.project_path=${JSON.stringify(invalidPath)} to be refused`,
    );
  }

  // `source.project_path` is only a fallback hint for multi-project sources: it must not override
  // explicit canonical `request.project_path` or verified job/history identity.
  const conflictingSourceObs: ObservationSnapshot = {
    ...defaultObs,
    sources: [{ ...defaultSourceWithNullPath, project_path: otherProject }],
  };
  assert.equal(
    doesObservationRequestMatchChat(validAdvisorCompletion, chat, {
      observation: conflictingSourceObs,
      jobHistories: [alphaJobHistory],
    }),
    true,
  );
  assert.equal(
    doesObservationRequestMatchChat(omittedScopeNoJobOnNullSource, chat, {
      observation: conflictingSourceObs,
      jobHistories: [alphaJobHistory],
    }),
    false,
  );

  // Conflicting unambiguous source.project_path without request.project_path must also refuse.
  const conflictingUnambiguousSourceObs: ObservationSnapshot = {
    ...defaultObs,
    sources: [{ ...defaultSourceWithNullPath, project_path: otherProject }],
    jobs: [otherProjectJob],
  };
  assert.equal(
    doesObservationRequestMatchChat(omittedScopeNoJobOnNullSource, chat, {
      observation: conflictingUnambiguousSourceObs,
      jobHistories: [],
    }),
    false,
  );

  // Legacy omitted `request.project_path` on a matching unambiguous source keeps backward compatibility.
  const unambiguousMatchingSource: ObservationSnapshot = {
    ...defaultObs,
    sources: [{ ...defaultSourceWithNullPath, project_path: alphaProject }],
    jobs: [alphaProjectJob],
  };
  assert.equal(
    doesObservationRequestMatchChat(omittedScopeNoJobOnNullSource, chat, {
      observation: unambiguousMatchingSource,
      jobHistories: [alphaJobHistory],
    }),
    true,
  );

  const views = buildChatActivityEventViews(selection, chat, {
    observation: defaultObs,
    job_histories: [alphaJobHistory],
  });
  const planningViews = views.filter((view) => 'planningRole' in view);
  assert.deepEqual(
    planningViews.map((view) => view.eventId),
    ['planning:advisor-jobless-exact-scope'],
  );
  assert.equal(planningViews[0]?.status, 'completed');
  assert.equal(planningViews[0]?.durationLabel, '2s');
});

test('multi-project source.project_path=/repo/a hint does not override canonical /repo/b request and verified job', () => {
  const repoA = '/repo/a';
  const repoB = '/repo/b';
  const chatBIdentity = makeIdentity('bnd_repo_b', UUID_BETA, repoB);
  const chatB = makeChatSummary(chatBIdentity, 'Repo B Thread');

  const verifiedJobBHistory = normalizeWorkspaceJobHistory({
    job: {
      source_id: chatBIdentity.source_id,
      job_id: 'job_repo_b',
      binding_id: chatBIdentity.binding_id,
      chat_identity: chatBIdentity,
      title: 'Repo B Verified Job',
      state: 'completed',
      approval_status: 'approved',
      execution_status: 'completed',
    },
  });
  const verifiedJobB = makeObservationJob({
    id: 'job_repo_b',
    source_id: chatBIdentity.source_id,
    binding_id: chatBIdentity.binding_id,
    thread_uuid: chatBIdentity.thread_uuid,
    project_path: repoB,
  });
  const sharedSourceHintObs: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 8_000,
    sources: [
      {
        id: chatBIdentity.source_id,
        label: 'Shared Multi-Repo Source',
        project_path: repoA,
        availability: 'available',
        message: null,
      },
    ],
    jobs: [verifiedJobB],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };

  const repoBCanonicalWithJob = makeObservationRequest({
    id: 'req-repo-b-job',
    operation: 'coworker.planning.advisor',
    job_id: 'job_repo_b',
    project_path: repoB,
    binding_id: chatBIdentity.binding_id,
    thread_uuid: chatBIdentity.thread_uuid,
    state: 'completed',
  });
  const repoBOmittedPathWithJob = makeObservationRequest({
    ...repoBCanonicalWithJob,
    id: 'req-repo-b-omitted-path-with-job',
    project_path: null,
    binding_id: null,
    thread_uuid: null,
  });
  const repoBCanonicalJobless = makeObservationRequest({
    ...repoBCanonicalWithJob,
    id: 'req-repo-b-jobless',
    job_id: null,
  });
  const omittedPathJoblessOnSharedHint = makeObservationRequest({
    ...repoBCanonicalJobless,
    id: 'req-omitted-path-jobless',
    project_path: null,
  });

  for (const ctx of [
    { observation: sharedSourceHintObs, jobHistories: [verifiedJobBHistory] },
    { observation: sharedSourceHintObs, jobHistories: [] },
  ]) {
    assert.equal(doesObservationRequestMatchChat(repoBCanonicalWithJob, chatB, ctx), true);
    assert.equal(doesObservationRequestMatchChat(repoBOmittedPathWithJob, chatB, ctx), true);
    assert.equal(doesObservationRequestMatchChat(repoBCanonicalJobless, chatB, ctx), true);
    assert.equal(
      doesObservationRequestMatchChat(omittedPathJoblessOnSharedHint, chatB, ctx),
      false,
    );
  }

  for (const rejectedReq of [
    { ...repoBCanonicalWithJob, id: 'req-bad-path', project_path: 'relative/repo-b' },
    { ...repoBCanonicalWithJob, id: 'req-wrong-path', project_path: repoA },
    { ...repoBCanonicalWithJob, id: 'req-wrong-source', source_id: 'src_other' },
    { ...repoBCanonicalWithJob, id: 'req-wrong-binding', binding_id: 'bnd_other' },
    { ...repoBCanonicalWithJob, id: 'req-wrong-thread', thread_uuid: UUID_ALPHA },
  ]) {
    assert.equal(
      doesObservationRequestMatchChat(rejectedReq, chatB, {
        observation: sharedSourceHintObs,
        jobHistories: [verifiedJobBHistory],
        knownChatJobIds: new Set(['job_repo_b']),
      }),
      false,
      `expected ${rejectedReq.id} to be refused`,
    );
  }
});

test('chat_work liveness requires exact fresh worker presence and never uses Doing, Blocked, failure, or awaiting-check as Working/NeedsYou', () => {
  const identity = makeIdentity('bnd_alpha_presence', UUID_ALPHA);
  const doingItem = makeBacklogItem({
    item_id: 'blg_worker',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-1',
    title: 'Run the isolated implementation pass',
  });
  const doneItem = makeBacklogItem({
    item_id: 'blg_done',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'done',
    current_execution_ref: 'exec-1',
  });
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingItem, doneItem],
        worker_presence: [makePresence({ item_id: 'blg_worker' })],
      }),
    ],
    identity,
  );

  const live = evaluateChatWorkLiveness(selection, null, { nowMs: CHAT_WORK_NOW_MS });
  assert.equal(live.isWorking, true);
  assert.equal(live.workingAgentCount, 1);
  assert.equal(live.statusLabel, 'Working');
  assert.equal(live.waitingForHuman, false);
  assert.deepEqual(live.agents.map((agent) => agent.role), ['worker']);
  assert.equal(live.agents[0]!.detail, 'Run the isolated implementation pass');

  const noPresence = evaluateChatWorkLiveness(
    selectWorkspaceChatWork([makeChatWork(identity, { items: [doingItem] })], identity),
    null,
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(noPresence.isWorking, false);
  assert.equal(noPresence.waitingForHuman, false);
  assert.equal(noPresence.statusLabel, null);

  const mismatchedExecution = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingItem],
        worker_presence: [
          makePresence({ item_id: 'blg_worker', execution_ref: 'exec-other' }),
        ],
      }),
    ],
    identity,
  );
  assert.equal(
    evaluateChatWorkLiveness(mismatchedExecution, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
    false,
  );

  const wrongBinding = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [{ ...doingItem, binding_id: 'bnd_other' }],
        worker_presence: [makePresence({ item_id: 'blg_worker' })],
      }),
    ],
    identity,
  );
  assert.equal(
    evaluateChatWorkLiveness(wrongBinding, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
    false,
  );

  const stalePresence = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingItem],
        worker_presence: [makePresence({ item_id: 'blg_worker', freshness: 'stale' })],
      }),
    ],
    identity,
  );
  assert.equal(
    evaluateChatWorkLiveness(stalePresence, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
    false,
  );

  const expiredPresence = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingItem],
        worker_presence: [
          makePresence({ item_id: 'blg_worker', expires_at_ms: CHAT_WORK_NOW_MS - 1 }),
        ],
      }),
    ],
    identity,
  );
  assert.equal(
    evaluateChatWorkLiveness(expiredPresence, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
    false,
  );

  const failedRefresh = evaluateChatWorkLiveness(selection, null, {
    nowMs: CHAT_WORK_NOW_MS,
    isStale: true,
  });
  assert.equal(failedRefresh.isWorking, false);
  assert.equal(failedRefresh.waitingForHuman, false);
  // Durable backlog remains present after the live claim is cleared.
  assert.deepEqual(failedRefresh.items.map((item) => item.item_id), ['blg_worker', 'blg_done']);
});

test('chat_work worker presence fails closed on missing, future, inconsistent, or aged samples', () => {
  const identity = makeIdentity('bnd_alpha_presence_validation', UUID_ALPHA);
  const doingItem = makeBacklogItem({
    item_id: 'blg_worker',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-1',
  });
  const livenessAt = (
    overrides: ChatWorkPresenceFixtureOverrides,
    nowMs: number = CHAT_WORK_NOW_MS,
  ) => {
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [doingItem],
          worker_presence: [makePresence({ item_id: 'blg_worker', ...overrides })],
        }),
      ],
      identity,
    );
    return evaluateChatWorkLiveness(selection, null, { nowMs });
  };

  assert.equal(livenessAt({}).isWorking, true);
  assert.equal(livenessAt({ expires_at_ms: 10_501 }).isWorking, true);
  assert.equal(livenessAt({ expires_at_ms: null }).isWorking, false);
  assert.equal(livenessAt({ expires_at_ms: 9_999 }).isWorking, false);
  assert.equal(livenessAt({ expires_at_ms: 15_001 }).isWorking, false);
  assert.equal(livenessAt({ sampled_at_ms: null }).isWorking, false);
  assert.equal(livenessAt({ sampled_at_ms: CHAT_WORK_NOW_MS + 1 }).isWorking, false);
  assert.equal(livenessAt({ sample_age_ms: null }).isWorking, false);
  assert.equal(livenessAt({ sample_age_ms: 5_001 }).isWorking, false);
  assert.equal(livenessAt({ max_sample_age_ms: 0 }).isWorking, false);
  assert.equal(livenessAt({ max_sample_age_ms: null }).isWorking, false);

  // The original sample ages even when the static sample_age_ms stays small.
  assert.equal(livenessAt({}, 10_000 + 5_000 + 1).isWorking, false);
  assert.equal(livenessAt({ expires_at_ms: 20_000 }, 10_000 + 5_000 + 1).isWorking, false);
});

test('chat_work event stream preserves the visible event id and offset across appends and head trimming', () => {
  const chatKey = 'wchat:event-anchor';
  const eventIds = Array.from({ length: 15 }, (_, index) => `evt_${index}`);
  const asEvents = (ids: readonly string[]) => ids.map((eventId) => ({ eventId }));
  const asLayoutItems = (ids: readonly string[]) => ids.map((item_id) => ({ item_id }));

  let state = createMessageScrollState(chatKey);
  const initialSync = syncChatWorkEventScroll(state, asEvents(eventIds));
  state = initialSync.state;
  assert.equal(initialSync.action.kind, 'scroll_to_bottom');
  assert.equal(state.mode, 'follow');

  const clientHeight = 260;
  const initialLayout = buildSimulatedLayout(asLayoutItems(eventIds), new Map(), 60, 8, 12, 12);
  const readingScrollTop = 234;
  state = handleMessageViewportScroll(
    state,
    {
      scrollTop: readingScrollTop,
      clientHeight,
      scrollHeight: initialLayout.scrollHeight,
    },
    measureItemsFromContentOffsets(initialLayout.layoutItems, readingScrollTop),
  );
  assert.equal(state.mode, 'reading');
  assert.deepEqual(state.anchor, { itemId: 'evt_3', offsetPx: -18 });

  const appendedIds = [...eventIds, 'evt_15', 'evt_16'];
  const appendSync = syncChatWorkEventScroll(state, asEvents(appendedIds));
  state = appendSync.state;
  assert.equal(appendSync.action.kind, 'preserve_anchor');
  if (appendSync.action.kind === 'preserve_anchor') {
    assert.equal(appendSync.action.anchorWasTrimmed, false);
    assert.deepEqual(appendSync.action.anchor, { itemId: 'evt_3', offsetPx: -18 });
  }
  assert.equal(state.pendingNewCount, 2);

  // Remove two events from the head while the anchor survives: the reducer
  // must compensate the shifted content, not merely restore the old scrollTop.
  const trimmedIds = appendedIds.slice(2);
  const trimSync = syncChatWorkEventScroll(state, asEvents(trimmedIds));
  state = trimSync.state;
  assert.equal(trimSync.action.kind, 'preserve_anchor');
  if (trimSync.action.kind === 'preserve_anchor') {
    assert.equal(trimSync.action.anchorWasTrimmed, false);
    assert.deepEqual(trimSync.action.anchor, { itemId: 'evt_3', offsetPx: -18 });
  }

  const trimmedLayout = buildSimulatedLayout(asLayoutItems(trimmedIds), new Map(), 60, 8, 12, 12);
  const measuredBeforeCompensation = measureItemsFromContentOffsets(
    trimmedLayout.layoutItems,
    readingScrollTop,
  );
  const anchorBeforeCompensation = measuredBeforeCompensation.find(
    (entry) => entry.itemId === 'evt_3',
  )!;
  const compensatedScrollTop = computeAnchorScrollTop(
    {
      scrollTop: readingScrollTop,
      clientHeight,
      scrollHeight: trimmedLayout.scrollHeight,
    },
    anchorBeforeCompensation.relativeTopPx,
    state.anchor!.offsetPx,
  );
  assert.equal(compensatedScrollTop, readingScrollTop - 2 * 68);
  const verifiedAfterTrim = measureItemsFromContentOffsets(
    trimmedLayout.layoutItems,
    compensatedScrollTop,
  ).find((entry) => entry.itemId === 'evt_3')!;
  assert.equal(verifiedAfterTrim.relativeTopPx, -18);

  // If head trimming evicts the anchored event itself, fall back to the next
  // surviving event with a zero offset.
  const evictedIds = trimmedIds.slice(3);
  const evictedSync = syncChatWorkEventScroll(state, asEvents(evictedIds));
  assert.equal(evictedSync.state.mode, 'reading');
  assert.equal(evictedSync.action.kind, 'preserve_anchor');
  if (evictedSync.action.kind === 'preserve_anchor') {
    assert.equal(evictedSync.action.anchorWasTrimmed, true);
    assert.deepEqual(evictedSync.action.anchor, { itemId: 'evt_5', offsetPx: 0 });
  }
});

test('chat_work event stream keeps per-chat anchors across chat switching', () => {
  const alphaEvents = Array.from({ length: 8 }, (_, index) => ({
    eventId: `alpha_${index}`,
  }));
  const betaEvents = [{ eventId: 'beta_0' }, { eventId: 'beta_1' }];
  let store = createMessageScrollStore('wchat:alpha');

  let alphaState = syncChatWorkEventScroll(
    getActiveChatScrollState(store),
    alphaEvents,
  ).state;
  const layout = buildSimulatedLayout(
    alphaEvents.map((event) => ({ item_id: event.eventId })),
    new Map(),
    60,
    8,
    12,
    12,
  );
  const readingScrollTop = 142;
  alphaState = handleMessageViewportScroll(
    alphaState,
    { scrollTop: readingScrollTop, clientHeight: 200, scrollHeight: layout.scrollHeight },
    measureItemsFromContentOffsets(layout.layoutItems, readingScrollTop),
  );
  assert.equal(alphaState.mode, 'reading');
  store = setChatScrollState(store, alphaState);

  store = switchMessageScrollChat(store, 'wchat:beta');
  const betaState = syncChatWorkEventScroll(
    getActiveChatScrollState(store),
    betaEvents,
  ).state;
  store = setChatScrollState(store, betaState);
  assert.equal(getActiveChatScrollState(store).mode, 'follow');
  assert.equal(getActiveChatScrollState(store).anchor, null);

  store = switchMessageScrollChat(store, 'wchat:alpha');
  const restored = getActiveChatScrollState(store);
  assert.equal(restored.mode, 'reading');
  assert.deepEqual(restored.anchor, alphaState.anchor);
  assert.equal(
    syncChatWorkEventScroll(restored, alphaEvents).action.kind,
    'preserve_anchor',
  );
});

test('chat_work event stream restores the reading anchor when content reflows above the viewport', () => {
  const eventIds = Array.from({ length: 12 }, (_, index) => `evt_${index}`);
  const asEvents = (ids: readonly string[]) => ids.map((eventId) => ({ eventId }));
  const asLayoutItems = (ids: readonly string[]) => ids.map((item_id) => ({ item_id }));

  let state = syncChatWorkEventScroll(
    createMessageScrollState('wchat:event-reflow'),
    asEvents(eventIds),
  ).state;
  const baseLayout = buildSimulatedLayout(asLayoutItems(eventIds), new Map(), 60, 8, 12, 12);
  const initialScrollTop = baseLayout.layoutItems[4]!.offsetTopPx + 22;
  state = handleMessageViewportScroll(
    state,
    {
      scrollTop: initialScrollTop,
      clientHeight: 220,
      scrollHeight: baseLayout.scrollHeight,
    },
    measureItemsFromContentOffsets(baseLayout.layoutItems, initialScrollTop),
  );
  assert.deepEqual(state.anchor, { itemId: 'evt_4', offsetPx: -22 });

  // Narrowing the viewport wraps every event, growing content above the anchor.
  const wrappedLayout = buildSimulatedLayout(asLayoutItems(eventIds), new Map(), 84, 8, 12, 12);
  const decision = resolveMessageViewportResize(
    state,
    {
      scrollTop: initialScrollTop,
      clientHeight: 260,
      scrollHeight: wrappedLayout.scrollHeight,
    },
    measureItemsFromContentOffsets(wrappedLayout.layoutItems, initialScrollTop),
  );
  assert.equal(decision.action.kind, 'restore_anchor');
  if (decision.action.kind === 'restore_anchor') {
    const afterReflow = measureItemsFromContentOffsets(
      wrappedLayout.layoutItems,
      decision.action.targetScrollTop,
    ).find((entry) => entry.itemId === 'evt_4')!;
    assert.equal(afterReflow.relativeTopPx, -22);
  }
  assert.equal(decision.state.anchor?.offsetPx, -22);
});

test('chat_work NeedsYou comes only from fresh native waiting flags with a single clear label', () => {
  const identity = makeIdentity('bnd_alpha_native_waiting', UUID_ALPHA);
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [
          makeBacklogItem({ status: 'blocked', blocked_reason: 'Waiting on a dependency.' }),
          makeBacklogItem({ item_id: 'blg_doing', status: 'doing' }),
        ],
        events: [
          makeBacklogEvent({
            event_id: 'blg_outcome:3',
            sequence: 3,
            item_id: 'blg_doing',
            revision: 3,
            action: 'worker_outcome_recorded',
            status: 'doing',
            outcome: 'success',
          }),
        ],
      }),
    ],
    identity,
  );

  const waiting = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'waiting_for_approval'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(waiting.waitingForHuman, true);
  assert.equal(waiting.isWorking, false);
  assert.equal(waiting.statusLabel, 'Waiting for you in Codex');
  assert.deepEqual(waiting.agents, []);
  assert.equal(buildChatWorkEventViews(selection)[0]!.actionLabel, 'Awaiting check');

  const waitingForInput = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'waiting_for_input'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(waitingForInput.waitingForHuman, true);

  const staleWaiting = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'waiting_for_approval'),
    { nowMs: CHAT_WORK_NOW_MS + 60_000 },
  );
  assert.equal(staleWaiting.waitingForHuman, false);
  assert.equal(staleWaiting.statusLabel, null);

  const working = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'working'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(working.isWorking, true);
  assert.equal(working.waitingForHuman, false);
  assert.equal(working.statusLabel, 'Working');

  const failedRefresh = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'waiting_for_approval'),
    { nowMs: CHAT_WORK_NOW_MS, refreshFailed: true },
  );
  assert.equal(failedRefresh.waitingForHuman, false);
  assert.equal(failedRefresh.isWorking, false);
});

test('chat_work backlog groups stay honest and bounded with truthful omitted counts', () => {
  const identity = makeIdentity('bnd_alpha_bounds', UUID_ALPHA);
  const statuses = ['todo', 'doing', 'done', 'blocked'] as const;
  const items = Array.from({ length: 205 }, (_, index) =>
    makeBacklogItem({
      item_id: `blg_${index}`,
      revision: index + 1,
      title: `Item ${index}`,
      status: statuses[index % statuses.length]!,
      blocked_reason: index % statuses.length === 3 ? `Reason ${index}` : null,
      completion_note: index % statuses.length === 2 ? `Checked ${index}` : null,
      completion_evidence: index % statuses.length === 2 ? `Evidence ${index}` : null,
      completed_at_ms: index % statuses.length === 2 ? 2_000 + index : null,
    }),
  );
  const events = Array.from({ length: 505 }, (_, index) =>
    makeBacklogEvent({
      event_id: `blg_${index}:${index + 1}`,
      sequence: 1_000 + index,
      item_id: `blg_${index}`,
      revision: index + 1,
    }),
  );
  const presence = Array.from({ length: 33 }, (_, index) =>
    makePresence({
      item_id: `blg_${index}`,
      execution_ref: `exec-${index}`,
    }),
  );
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items,
        events,
        worker_presence: presence,
        bounds: { items_omitted: 7, events_omitted: 9, presence_omitted: 11 },
      }),
    ],
    identity,
  );

  assert.equal(selection.items.length, 200);
  assert.equal(selection.events.length, 500);
  assert.equal(selection.presence.length, 32);
  assert.equal(selection.bounds.itemsOmitted, 12);
  assert.equal(selection.bounds.eventsOmitted, 14);
  assert.equal(selection.bounds.presenceOmitted, 12);
  assert.ok(selection.notices.some((notice) => notice.includes('older backlog items')));
  assert.ok(selection.notices.some((notice) => notice.includes('older stream events')));
  assert.ok(selection.notices.some((notice) => notice.includes('worker presence records')));

  const groups = buildChatWorkBacklogGroups(selection);
  assert.deepEqual(groups.map((group) => group.status), ['todo', 'doing', 'done', 'blocked']);
  assert.ok(groups.every((group) => group.items.every((item) => item.status === group.status)));
  const blocked = groups.find((group) => group.status === 'blocked')!.items[0]!;
  assert.ok(blocked.blockedReason !== null);
  const done = groups.find((group) => group.status === 'done')!.items[0]!;
  assert.ok(done.completionNote !== null);
  assert.ok(done.completionEvidence !== null);
});

test('chat_work liveness index keeps native-sample state visible without inventing legacy backlog liveness', () => {
  const alphaIdentity = makeIdentity('bnd_alpha_index', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_beta_index', UUID_BETA);
  const alphaWork = makeChatWork(alphaIdentity, {
    items: [makeBacklogItem({ status: 'doing' })],
  });
  const index = buildChatWorkLivenessIndex(
    [alphaWork],
    [makeNativeSample(alphaIdentity, 'working'), makeNativeSample(betaIdentity, 'waiting_for_input')],
    { nowMs: CHAT_WORK_NOW_MS },
  );

  const alpha = index.get(resolveMessageScrollChatKey(alphaIdentity))!;
  assert.equal(alpha.isWorking, true);
  assert.equal(alpha.waitingForHuman, false);

  const beta = index.get(resolveMessageScrollChatKey(betaIdentity))!;
  assert.equal(beta.waitingForHuman, true);
  assert.equal(beta.isWorking, false);
  assert.equal(beta.workAvailable, false);
  assert.deepEqual(beta.items, []);

  const summary = summarizeChatWorkLiveness(index);
  assert.equal(summary.workingAgentCount, 1);
  assert.equal(summary.waitingChatCount, 1);
  assert.equal(summary.workingChatCount, 1);
});

// ---------------------------------------------------------------------------
// Worker execution footers: exact joins, latest-event-only, TTL, and timing
// ---------------------------------------------------------------------------

function selectExecutionFixture(
  identity: WorkspaceChatIdentity,
  options: {
    item?: Partial<ChatWorkItem>;
    execution?: ChatWorkExecutionFixtureOverrides;
    executionRef?: string;
    eventOverrides?: Partial<ChatWorkEvent>;
    presence?: ChatWorkPresence[];
  } = {},
): {
  selection: ReturnType<typeof selectWorkspaceChatWork>;
  views: ReturnType<typeof buildChatWorkEventViews>;
  summary: ChatWorkExecutionSummaryView;
} {
  const executionRef = options.executionRef ?? 'exec-1';
  const item = makeBacklogItem({
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: executionRef,
    ...options.item,
  });
  const execution = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: executionRef,
    ...options.execution,
  });
  const event = makeBacklogEvent({
    event_id: `${item.item_id}:${item.revision}`,
    item_id: item.item_id,
    revision: item.revision,
    action: 'worker_outcome_recorded',
    status: item.status,
    execution_ref: executionRef,
    sequence: item.revision,
    recorded_at_ms: 1_000 + item.revision,
    ...options.eventOverrides,
  });
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [item],
        events: [event],
        worker_presence: options.presence ?? [],
        worker_executions: [execution],
      }),
    ],
    identity,
  );
  const views = buildChatWorkEventViews(selection);
  const summary = views.find((view) => view.workerExecution !== null)!.workerExecution!;
  return { selection, views, summary };
}

test('worker execution footers join exact item/binding attempts and render only on the latest event for that execution', () => {
  const identity = makeIdentity('bnd_alpha_execution_join', UUID_ALPHA);
  const firstItem = makeBacklogItem({
    item_id: 'blg_exec_first',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-1',
  });
  const secondItem = makeBacklogItem({
    item_id: 'blg_exec_second',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'The first attempt was not accepted.',
    current_execution_ref: 'exec-2',
  });
  const events = [
    makeBacklogEvent({
      event_id: 'blg_exec_first:1',
      sequence: 1,
      item_id: 'blg_exec_first',
      revision: 1,
      action: 'worker_execution_attached',
      status: 'doing',
      execution_ref: 'exec-1',
      recorded_at_ms: 1_100,
    }),
    makeBacklogEvent({
      event_id: 'blg_exec_first:2',
      sequence: 2,
      item_id: 'blg_exec_first',
      revision: 2,
      action: 'worker_outcome_recorded',
      status: 'doing',
      execution_ref: 'exec-1',
      outcome: 'success',
      recorded_at_ms: 1_200,
    }),
    makeBacklogEvent({
      event_id: 'blg_exec_second:1',
      sequence: 3,
      item_id: 'blg_exec_second',
      revision: 1,
      action: 'worker_outcome_recorded',
      status: 'blocked',
      execution_ref: 'exec-2',
      outcome: 'failure',
      recorded_at_ms: 1_300,
    }),
  ];
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [firstItem, secondItem],
        events,
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: 'blg_exec_first',
            execution_ref: 'exec-1',
            outcome: 'success',
            worker_status: 'completed',
            acceptance: 'pending_review',
            started_at_ms: 900,
            finished_at_ms: 1_200,
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_exec_second',
            execution_ref: 'exec-2',
            outcome: 'failure',
            worker_status: 'failed',
            started_at_ms: 1_000,
            finished_at_ms: 1_300,
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_missing_item',
            execution_ref: 'exec-orphan',
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_exec_first',
            execution_ref: 'exec-runtime-binding',
            session_binding_id: 'bnd_runtime_approved',
            approval_id: 'apr_runtime',
            task_id: 'tsk_runtime',
            run_id: 'run_runtime',
            worker_id: 'wrk_runtime',
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_other_chat',
            execution_ref: 'exec-cross-chat',
          }),
        ],
      }),
    ],
    identity,
  );

  assert.deepEqual(
    selection.executions.map((execution) => execution.execution_ref),
    ['exec-1', 'exec-2', 'exec-runtime-binding'],
  );
  const runtimeBindingExecution = selection.executions.find(
    (execution) => execution.execution_ref === 'exec-runtime-binding',
  );
  assert.equal(runtimeBindingExecution?.session_binding_id, 'bnd_runtime_approved');
  assert.equal(runtimeBindingExecution?.approval_id, 'apr_runtime');
  assert.equal(runtimeBindingExecution?.task_id, 'tsk_runtime');
  assert.equal(runtimeBindingExecution?.run_id, 'run_runtime');
  assert.equal(runtimeBindingExecution?.worker_id, 'wrk_runtime');
  assert.equal(selection.bounds.executionsOmitted, 2);
  assert.ok(
    selection.notices.some((notice) => notice.includes('worker execution records')),
  );

  const views = buildChatWorkEventViews(selection);
  assert.equal(views[0]!.workerExecution, null);
  assert.equal(views[0]!.actionLabel, 'Worker dispatched');
  assert.equal(views[1]!.workerExecution?.executionRef, 'exec-1');
  assert.equal(views[1]!.workerExecution?.statusKey, 'checking');
  assert.equal(views[2]!.workerExecution?.executionRef, 'exec-2');
  assert.equal(views[2]!.workerExecution?.statusKey, 'failed');
  assert.equal(views[1]!.actionLabel, 'Awaiting check');
  assert.equal(views[1]!.outcome, 'success');
});

test('worker execution summary displays Neon and XHigh while retaining the recorded model ID', () => {
  const identity = makeIdentity('bnd_alpha_neon', UUID_ALPHA);
  const execution = makeExecution(identity.binding_id, {
    provider: 'antigravity',
    requested_model: 'gemini-4-neon-low',
    observed_model: 'gemini-4-neon-xhigh',
    requested_effort: 'low',
    observed_effort: 'xhigh',
  });
  const item = makeBacklogItem({
    item_id: execution.item_id,
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: execution.execution_ref,
  });
  const selection = selectWorkspaceChatWork(
    [makeChatWork(identity, { items: [item], worker_executions: [execution] })],
    identity,
  );
  const [summary] = buildChatWorkExecutionSummaries(selection);
  assert.ok(summary);
  assert.equal(summary.modelRaw, 'gemini-4-neon-xhigh');
  assert.equal(summary.model, 'Gemini 4 Neon');
  assert.equal(summary.effort, 'XHigh');
});

test('worker execution summaries keep the Flash 4.1 label separate from Max and prefer observed identity', () => {
  const identity = makeIdentity('bnd_alpha_execution_model_labels', UUID_ALPHA);
  const requestedItem = makeBacklogItem({
    item_id: 'blg_flash_requested',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-requested',
  });
  const observedItem = makeBacklogItem({
    item_id: 'blg_flash_observed',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-observed',
  });
  const missingItem = makeBacklogItem({
    item_id: 'blg_model_missing',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-missing',
  });

  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [requestedItem, observedItem, missingItem],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: requestedItem.item_id,
            execution_ref: 'exec-requested',
            provider: 'opencode',
            requested_model: 'deepseek-v4.1-flash',
            observed_model: null,
            requested_effort: 'max',
            observed_effort: null,
          }),
          makeExecution(identity.binding_id, {
            item_id: observedItem.item_id,
            execution_ref: 'exec-observed',
            provider: 'opencode',
            requested_model: 'deepseek-v4.1-flash',
            observed_model: 'deepseek-v4.1-flash-pro',
            requested_effort: 'max',
            observed_effort: null,
          }),
          makeExecution(identity.binding_id, {
            item_id: missingItem.item_id,
            execution_ref: 'exec-missing',
            provider: 'opencode-go',
            requested_model: null,
            observed_model: null,
            requested_effort: null,
            observed_effort: null,
          }),
        ],
      }),
    ],
    identity,
  );

  const summaries = buildChatWorkExecutionSummaries(selection);
  const byRef = new Map(
    summaries.map((summary) => [summary.executionRef, summary] as const),
  );
  assert.equal(summaries.length, 3);

  const requested = byRef.get('exec-requested');
  assert.ok(requested);
  assert.equal(requested.provider, 'opencode');
  assert.equal(requested.modelRaw, 'deepseek-v4.1-flash');
  assert.equal(requested.model, 'DeepSeek Flash 4.1');
  assert.equal(requested.effort, 'Max');

  const observed = byRef.get('exec-observed');
  assert.ok(observed);
  assert.equal(observed.provider, 'opencode');
  assert.equal(observed.modelRaw, 'deepseek-v4.1-flash-pro');
  assert.equal(observed.model, 'DeepSeek · deepseek-v4.1-flash-pro');
  assert.notEqual(observed.model, 'DeepSeek Flash 4.1');
  // The observed model wins independently of effort. Preserve the existing
  // requested-effort fallback when no observed effort was recorded.
  assert.equal(observed.effort, 'Max');

  const missing = byRef.get('exec-missing');
  assert.ok(missing);
  assert.equal(missing.provider, 'opencode-go');
  assert.equal(missing.modelRaw, null);
  assert.equal(missing.model, null);
  assert.equal(missing.effort, null);
});

test('worker execution status keeps dispatch, checking, guarded Done, rejection, cancellation, failure, timeout, and unknown distinct', () => {
  const identity = makeIdentity('bnd_alpha_execution_status', UUID_ALPHA);

  const dispatched = selectExecutionFixture(identity, {
    item: { item_id: 'blg_dispatch', current_execution_ref: 'exec-1' },
    execution: { worker_status: 'running', outcome: null },
  });
  assert.equal(dispatched.summary.statusKey, 'dispatched');
  assert.equal(dispatched.summary.statusLabel, 'Dispatched');
  assert.equal(
    evaluateChatWorkLiveness(dispatched.selection, null, { nowMs: CHAT_WORK_NOW_MS })
      .isWorking,
    false,
  );

  const checking = selectExecutionFixture(identity, {
    item: { item_id: 'blg_checking', current_execution_ref: 'exec-1' },
    execution: {
      worker_status: 'completed',
      outcome: 'success',
      acceptance: 'pending_review',
    },
  });
  assert.equal(checking.summary.statusKey, 'checking');
  assert.equal(checking.summary.statusLabel, 'Awaiting check');
  assert.equal(checking.summary.tone, 'neutral');

  const completed = selectExecutionFixture(identity, {
    item: {
      item_id: 'blg_done',
      status: 'done',
      current_execution_ref: 'exec-1',
      completion_note: 'Checked and accepted',
      completion_evidence: 'Inspected the delivered artifact',
      completed_at_ms: 1_500,
    },
    execution: {
      worker_status: 'completed',
      outcome: 'success',
      acceptance: 'accepted',
      finished_at_ms: 1_400,
    },
  });
  assert.equal(completed.summary.statusKey, 'completed');
  assert.equal(completed.summary.statusLabel, 'Completed');

  const rejected = selectExecutionFixture(identity, {
    item: {
      item_id: 'blg_rejected',
      status: 'blocked',
      blocked_reason: 'Coworker acceptance failed.',
      current_execution_ref: 'exec-1',
    },
    execution: {
      worker_status: 'completed',
      outcome: 'success',
      acceptance: 'rejected',
    },
  });
  assert.equal(rejected.summary.statusKey, 'rejected');
  assert.equal(rejected.summary.statusLabel, 'Rejected');

  const cancelled = selectExecutionFixture(identity, {
    item: { item_id: 'blg_cancelled', current_execution_ref: 'exec-1' },
    execution: { worker_status: 'cancelled', outcome: null },
  });
  assert.equal(cancelled.summary.statusKey, 'cancelled');
  assert.equal(cancelled.summary.statusLabel, 'Cancelled');

  const failed = selectExecutionFixture(identity, {
    item: { item_id: 'blg_failed', current_execution_ref: 'exec-1' },
    execution: { worker_status: 'failed', outcome: 'failure' },
  });
  assert.equal(failed.summary.statusKey, 'failed');
  assert.equal(failed.summary.statusLabel, 'Failed');

  const timedOut = selectExecutionFixture(identity, {
    item: { item_id: 'blg_timeout', current_execution_ref: 'exec-1' },
    execution: { worker_status: 'timed_out', outcome: 'failure' },
  });
  assert.equal(timedOut.summary.statusKey, 'timed_out');
  assert.equal(timedOut.summary.statusLabel, 'Timed out');

  const unknown = selectExecutionFixture(identity, {
    item: { item_id: 'blg_unknown', current_execution_ref: 'exec-1' },
    execution: { worker_status: 'unknown', outcome: 'unknown' },
  });
  assert.equal(unknown.summary.statusKey, 'unknown');
  assert.equal(unknown.summary.statusLabel, 'Unknown');

  assert.equal(
    new Set([
      dispatched.summary.statusKey,
      checking.summary.statusKey,
      completed.summary.statusKey,
      rejected.summary.statusKey,
      cancelled.summary.statusKey,
      failed.summary.statusKey,
      timedOut.summary.statusKey,
      unknown.summary.statusKey,
    ]).size,
    8,
  );
});

test('accepted Done stays terminal while a new attempt after rejection or failure keeps its own footer', () => {
  const identity = makeIdentity('bnd_alpha_execution_attempts', UUID_ALPHA);
  const rejectedItem = makeBacklogItem({
    item_id: 'blg_retry',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-2',
    revision: 4,
    title: 'Run the corrected attempt',
  });
  const doneItem = makeBacklogItem({
    item_id: 'blg_terminal',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'done',
    current_execution_ref: 'exec-3',
    revision: 7,
    completion_note: 'Kontrol edildi ve kabul edildi',
    completion_evidence: 'Teslim edilen dosya incelendi',
    completed_at_ms: 2_000,
  });
  const events = [
    makeBacklogEvent({
      event_id: 'blg_retry:2',
      sequence: 20,
      item_id: 'blg_retry',
      revision: 2,
      action: 'worker_outcome_recorded',
      status: 'blocked',
      execution_ref: 'exec-1',
      outcome: 'success',
      recorded_at_ms: 1_200,
    }),
    makeBacklogEvent({
      event_id: 'blg_retry:4',
      sequence: 22,
      item_id: 'blg_retry',
      revision: 4,
      action: 'worker_outcome_recorded',
      status: 'doing',
      execution_ref: 'exec-2',
      outcome: 'failure',
      recorded_at_ms: 1_800,
    }),
    makeBacklogEvent({
      event_id: 'blg_terminal:7',
      sequence: 30,
      item_id: 'blg_terminal',
      revision: 7,
      action: 'completed',
      status: 'done',
      execution_ref: 'exec-3',
      recorded_at_ms: 2_000,
    }),
  ];
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [rejectedItem, doneItem],
        events,
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: 'blg_retry',
            execution_ref: 'exec-1',
            outcome: 'success',
            worker_status: 'completed',
            acceptance: 'rejected',
            started_at_ms: 1_000,
            finished_at_ms: 1_200,
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_retry',
            execution_ref: 'exec-2',
            outcome: 'failure',
            worker_status: 'failed',
            started_at_ms: 1_600,
            finished_at_ms: 1_800,
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_terminal',
            execution_ref: 'exec-3',
            outcome: 'success',
            worker_status: 'completed',
            acceptance: 'accepted',
            started_at_ms: 1_700,
            finished_at_ms: 2_000,
            duration_ms: 300,
          }),
        ],
      }),
    ],
    identity,
  );

  const firstViews = buildChatWorkEventViews(selection);
  assert.deepEqual(
    firstViews.map((view) => view.workerExecution?.statusKey ?? null),
    ['rejected', 'failed', 'completed'],
  );
  assert.equal(firstViews[0]!.workerExecution?.executionRef, 'exec-1');
  assert.equal(firstViews[1]!.workerExecution?.executionRef, 'exec-2');
  assert.equal(firstViews[2]!.workerExecution?.executionRef, 'exec-3');

  // Rebuilding the same durable snapshot never reopens Done, adds a footer, or
  // turns an execution into live Working.
  const secondViews = buildChatWorkEventViews(selection);
  assert.deepEqual(secondViews, firstViews);
  assert.equal(
    evaluateChatWorkLiveness(selection, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
    false,
  );
  assert.deepEqual(
    buildChatWorkBacklogGroups(selection)
      .find((group) => group.status === 'done')!
      .items.map((item) => item.completionNote),
    ['Kontrol edildi ve kabul edildi'],
  );
});

test('old payloads default to no footers and duplicate executions keep one footer with terminal duration', () => {
  const identity = makeIdentity('bnd_alpha_execution_duplicates', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_duplicate',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'The attempt failed.',
    current_execution_ref: 'exec-1',
  });
  const event = makeBacklogEvent({
    event_id: 'blg_duplicate:2',
    sequence: 2,
    item_id: 'blg_duplicate',
    revision: 2,
    action: 'worker_outcome_recorded',
    status: 'blocked',
    execution_ref: 'exec-1',
    outcome: 'failure',
    recorded_at_ms: 1_200,
  });
  const legacySelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [item],
        events: [event],
      }),
    ],
    identity,
  );
  assert.deepEqual(legacySelection.executions, []);
  assert.equal(legacySelection.bounds.executionsOmitted, 0);
  assert.equal(buildChatWorkEventViews(legacySelection)[0]!.workerExecution, null);

  const staleDuplicate = makeExecution(identity.binding_id, {
    item_id: 'blg_duplicate',
    execution_ref: 'exec-1',
    worker_status: 'running',
    outcome: null,
    started_at_ms: 1_000,
  });
  const terminalDuplicate = makeExecution(identity.binding_id, {
    item_id: 'blg_duplicate',
    execution_ref: 'exec-1',
    worker_status: 'failed',
    outcome: 'failure',
    started_at_ms: 1_000,
    finished_at_ms: 5_200,
    duration_ms: 4_200,
  });
  const refreshSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [item],
        events: [event, event],
        worker_executions: [
          staleDuplicate,
          terminalDuplicate,
          terminalDuplicate,
        ],
      }),
    ],
    identity,
  );
  assert.equal(refreshSelection.executions.length, 1);
  assert.equal(refreshSelection.events.length, 1);
  const views = buildChatWorkEventViews(refreshSelection);
  assert.equal(views.length, 1);
  assert.equal(views[0]!.workerExecution?.executionRef, 'exec-1');
  const footer = views[0]!.workerExecution!;
  assert.equal(footer.duration?.kind, 'final');
  assert.equal(resolveChatWorkExecutionDurationLabel(footer, CHAT_WORK_NOW_MS), '4.2s');
  assert.equal(resolveChatWorkExecutionDurationLabel(footer, 9_999_999), '4.2s');
  assert.deepEqual(buildChatWorkEventViews(refreshSelection), views);
});

test('conflicting same-ref execution identities are quarantined instead of resurrecting a footer', () => {
  const identity = makeIdentity('bnd_alpha_execution_identity_conflict', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_identity_conflict',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'Attempt identity conflicts must stay quarantined.',
    current_execution_ref: 'exec-1',
  });
  const event = makeBacklogEvent({
    event_id: 'blg_identity_conflict:2',
    sequence: 2,
    item_id: item.item_id,
    revision: 2,
    action: 'worker_outcome_recorded',
    status: 'blocked',
    execution_ref: 'exec-1',
    outcome: 'failure',
    recorded_at_ms: 1_200,
  });
  const base = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: 'exec-1',
    session_binding_id: 'bnd_runtime_exact_attempt',
    worker_status: 'running',
    outcome: null,
    started_at_ms: 9_000,
  });
  const terminal = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: 'exec-1',
    session_binding_id: 'bnd_runtime_exact_attempt',
    worker_status: 'completed',
    outcome: 'success',
    acceptance: 'accepted',
    observed_model: 'gemini-4-argon-high',
    started_at_ms: 9_000,
    finished_at_ms: 13_200,
    duration_ms: 4_200,
  });
  const selectionFor = (executions: ChatWorkExecution[]) =>
    selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [item],
          events: [event],
          worker_executions: executions,
        }),
      ],
      identity,
    );

  const enriched = selectionFor([base, terminal, terminal]);
  assert.equal(enriched.executions.length, 1);
  assert.equal(enriched.executions[0]!.worker_status, 'completed');
  assert.equal(enriched.executions[0]!.duration_ms, 4_200);
  assert.equal(enriched.executions[0]!.observed_model, 'gemini-4-argon-high');
  assert.equal(
    enriched.executions[0]!.session_binding_id,
    'bnd_runtime_exact_attempt',
  );
  assert.notEqual(enriched.executions[0]!.session_binding_id, identity.binding_id);
  const enrichedFooter = buildChatWorkEventViews(enriched)[0]!.workerExecution!;
  assert.equal(enrichedFooter.duration?.kind, 'final');
  assert.equal(resolveChatWorkExecutionDurationLabel(enrichedFooter, CHAT_WORK_NOW_MS), '4.2s');

  const conflictingOverrides: ChatWorkExecutionFixtureOverrides[] = [
    { approval_id: 'approval-other' },
    { task_id: 'task-other' },
    { session_binding_id: 'bnd_runtime_other' },
    { run_id: 'run-other' },
    { worker_id: 'worker-other' },
  ];
  for (const override of conflictingOverrides) {
    const conflicting = makeExecution(identity.binding_id, {
      item_id: item.item_id,
      execution_ref: 'exec-1',
      session_binding_id: 'bnd_runtime_exact_attempt',
      worker_status: 'completed',
      outcome: 'success',
      acceptance: 'accepted',
      observed_model: 'gemini-4-argon-high',
      started_at_ms: 9_000,
      finished_at_ms: 13_200,
      duration_ms: 4_200,
      ...override,
    });

    const conflicted = selectionFor([base, conflicting]);
    assert.deepEqual(
      conflicted.executions,
      [],
      'a contradicting immutable identity must quarantine the whole same-ref group',
    );
    assert.equal(
      conflicted.bounds.executionsOmitted,
      2,
      'both rows in the conflicting group must be counted as omitted',
    );
    assert.equal(
      buildChatWorkEventViews(conflicted)[0]!.workerExecution,
      null,
      'quarantined metadata must not transfer Completed, model, or duration',
    );

    const resurrected = selectionFor([base, conflicting, terminal]);
    assert.deepEqual(
      resurrected.executions,
      [],
      'a later third row must not resurrect a quarantined group',
    );
    assert.equal(
      resurrected.bounds.executionsOmitted,
      3,
      'the later row must also be counted as omitted',
    );
    assert.equal(buildChatWorkEventViews(resurrected)[0]!.workerExecution, null);
  }
});

test('raw chat_work composition quarantines cross-entry execution identity conflicts in either order', () => {
  const identity = makeIdentity('bnd_alpha_cross_entry_identity', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_cross_entry_identity',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'Attempt identity conflicts must stay quarantined.',
    current_execution_ref: 'exec-1',
  });
  const event = makeBacklogEvent({
    event_id: 'blg_cross_entry_identity:2',
    sequence: 2,
    item_id: item.item_id,
    revision: 2,
    action: 'worker_outcome_recorded',
    status: 'blocked',
    execution_ref: 'exec-1',
    outcome: 'failure',
    recorded_at_ms: 1_200,
  });
  const base = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: 'exec-1',
    session_binding_id: 'bnd_runtime_cross_entry',
    worker_status: 'running',
    outcome: null,
    started_at_ms: 9_000,
  });
  const terminal = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: 'exec-1',
    session_binding_id: 'bnd_runtime_cross_entry',
    worker_status: 'completed',
    outcome: 'success',
    acceptance: 'accepted',
    observed_model: 'gemini-4-argon-high',
    started_at_ms: 9_000,
    finished_at_ms: 13_200,
    duration_ms: 4_200,
  });
  const workFor = (executions: ChatWorkExecution[]) =>
    makeChatWork(identity, {
      items: [item],
      events: [event],
      worker_executions: executions,
    });
  const selectionFor = (entries: WorkspaceChatWork[]) =>
    selectWorkspaceChatWork(entries, identity);

  const conflictingOverrides: ReadonlyArray<[string, ChatWorkExecutionFixtureOverrides]> = [
    ['approval_id', { approval_id: 'approval-cross-entry-other' }],
    ['task_id', { task_id: 'task-cross-entry-other' }],
    ['session_binding_id', { session_binding_id: 'bnd_runtime_cross_entry_other' }],
    ['run_id', { run_id: 'run-cross-entry-other' }],
    ['worker_id', { worker_id: 'worker-cross-entry-other' }],
  ];
  for (const [field, override] of conflictingOverrides) {
    const conflicting = makeExecution(identity.binding_id, {
      item_id: item.item_id,
      execution_ref: 'exec-1',
      session_binding_id: 'bnd_runtime_cross_entry',
      worker_status: 'completed',
      outcome: 'success',
      acceptance: 'accepted',
      observed_model: 'gemini-4-argon-high',
      started_at_ms: 9_000,
      finished_at_ms: 13_200,
      duration_ms: 4_200,
      ...override,
    });

    for (const order of ['conflict_first', 'terminal_first'] as const) {
      const entries =
        order === 'conflict_first'
          ? [workFor([base, conflicting]), workFor([terminal])]
          : [workFor([terminal]), workFor([base, conflicting])];
      const selection = selectionFor(entries);
      assert.deepEqual(
        selection.executions,
        [],
        `${field} contradiction must quarantine the whole group across ${order} entries`,
      );
      assert.equal(
        selection.bounds.executionsOmitted,
        3,
        `${field} must count every omitted raw row once across ${order} entries`,
      );
      assert.equal(
        buildChatWorkEventViews(selection)[0]!.workerExecution,
        null,
        `${field} contradiction must not transfer Completed, model, or duration`,
      );

      const liveness = evaluateChatWorkLiveness(
        selection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      );
      assert.equal(liveness.isWorking, false);
      assert.equal(liveness.waitingForHuman, false);
      const indexEntry = buildChatWorkLivenessIndex(
        entries,
        [makeNativeSample(identity, 'idle')],
        { nowMs: CHAT_WORK_NOW_MS },
      ).get(resolveMessageScrollChatKey(identity))!;
      assert.equal(indexEntry.isWorking, false);
      assert.equal(indexEntry.waitingForHuman, false);
    }
  }

  const unrelated = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: 'exec-keep',
    approval_id: 'approval-keep',
    task_id: 'task-keep',
    run_id: 'run-keep',
    worker_id: 'worker-keep',
    worker_status: 'failed',
    outcome: 'failure',
    started_at_ms: 9_100,
    finished_at_ms: 9_600,
    duration_ms: 500,
  });
  const mixed = selectionFor([
    workFor([
      base,
      makeExecution(identity.binding_id, {
        item_id: item.item_id,
        execution_ref: 'exec-1',
        approval_id: 'approval-cross-entry-other',
        session_binding_id: 'bnd_runtime_cross_entry',
      }),
    ]),
    workFor([terminal, unrelated]),
  ]);
  assert.deepEqual(
    mixed.executions.map((execution) => execution.execution_ref),
    ['exec-keep'],
    'an unrelated exact execution in the same chat must survive the quarantined group',
  );
  assert.equal(mixed.bounds.executionsOmitted, 3);

  const betaIdentity = makeIdentity('bnd_beta_cross_entry_identity', UUID_BETA);
  const betaItem = makeBacklogItem({
    item_id: 'blg_beta_cross_entry_identity',
    binding_id: betaIdentity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'Independent chat work stays isolated.',
    current_execution_ref: 'exec-beta',
  });
  const betaExecution = makeExecution(betaIdentity.binding_id, {
    item_id: betaItem.item_id,
    execution_ref: 'exec-beta',
    approval_id: 'approval-beta',
    task_id: 'task-beta',
    run_id: 'run-beta',
    worker_id: 'worker-beta',
  });
  const betaSelection = selectWorkspaceChatWork(
    [
      workFor([base]),
      makeChatWork(betaIdentity, {
        items: [betaItem],
        events: [],
        worker_executions: [betaExecution],
      }),
    ],
    betaIdentity,
  );
  assert.deepEqual(
    betaSelection.executions.map((execution) => execution.execution_ref),
    ['exec-beta'],
  );
  assert.equal(betaSelection.bounds.executionsOmitted, 0);

  const enriched = selectionFor([workFor([base]), workFor([terminal])]);
  assert.equal(enriched.executions.length, 1);
  assert.equal(enriched.executions[0]!.worker_status, 'completed');
  assert.equal(enriched.executions[0]!.duration_ms, 4_200);
  assert.equal(enriched.executions[0]!.observed_model, 'gemini-4-argon-high');
  assert.equal(enriched.executions[0]!.session_binding_id, 'bnd_runtime_cross_entry');
  assert.notEqual(enriched.executions[0]!.session_binding_id, identity.binding_id);
  assert.equal(enriched.bounds.executionsOmitted, 0);
  const enrichedFooter = buildChatWorkEventViews(enriched)[0]!.workerExecution!;
  assert.equal(resolveChatWorkExecutionDurationLabel(enrichedFooter, CHAT_WORK_NOW_MS), '4.2s');
});

test('worker footer pairs presence only when every available immutable identity field matches', () => {
  const identity = makeIdentity('bnd_alpha_execution_presence_identity', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_presence_identity',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-1',
  });
  const event = makeBacklogEvent({
    event_id: 'blg_presence_identity:3',
    sequence: 3,
    item_id: item.item_id,
    revision: 3,
    status: 'doing',
    execution_ref: 'exec-1',
    recorded_at_ms: 1_300,
  });
  const execution = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: 'exec-1',
    session_binding_id: 'bnd_runtime_presence_identity',
    worker_status: 'running',
    outcome: null,
    started_at_ms: 9_000,
  });
  const selectionFor = (presence: ChatWorkPresence) =>
    selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [item],
          events: [event],
          worker_presence: [presence],
          worker_executions: [execution],
        }),
      ],
      identity,
    );

  const exact = selectionFor(
    makePresence({ item_id: item.item_id, execution_ref: 'exec-1' }),
  );
  assert.equal(exact.executions[0]!.session_binding_id, 'bnd_runtime_presence_identity');
  assert.notEqual(exact.executions[0]!.session_binding_id, identity.binding_id);
  const exactFooter = buildChatWorkEventViews(exact)[0]!.workerExecution!;
  assert.notEqual(exactFooter.presence, null);
  const exactState = resolveChatWorkExecutionFooterState(exactFooter, CHAT_WORK_NOW_MS);
  assert.equal(exactState.statusKey, 'working');
  assert.equal(exactState.durationLabel, 'Elapsed 1.5s');

  const manualConflict = {
    ...exact,
    executions: [
      execution,
      makeExecution(identity.binding_id, {
        item_id: item.item_id,
        execution_ref: 'exec-1',
        session_binding_id: 'bnd_runtime_presence_identity',
        approval_id: 'approval-other',
        worker_status: 'completed',
        outcome: 'success',
        acceptance: 'accepted',
        duration_ms: 4_200,
      }),
    ],
  };
  assert.deepEqual(
    buildChatWorkExecutionSummaries(manualConflict),
    [],
    'a caller-constructed conflicting identity group must not transfer Completed, model, or duration',
  );

  for (const presence of [
    makePresence({
      item_id: item.item_id,
      execution_ref: 'exec-1',
      approval_id: 'approval-other',
    }),
    makePresence({
      item_id: item.item_id,
      execution_ref: 'exec-1',
      task_id: 'task-other',
    }),
    makePresence({
      item_id: item.item_id,
      execution_ref: 'exec-1',
      run_id: 'run-other',
    }),
    makePresence({
      item_id: item.item_id,
      execution_ref: 'exec-1',
      worker_id: 'worker-other',
    }),
  ]) {
    const selection = selectionFor(presence);
    const footer = buildChatWorkEventViews(selection)[0]!.workerExecution!;
    assert.equal(
      footer.presence,
      null,
      'a mismatched immutable identity must not attach presence to the footer',
    );
    const state = resolveChatWorkExecutionFooterState(footer, CHAT_WORK_NOW_MS);
    assert.equal(state.statusKey, 'dispatched');
    assert.equal(state.duration, null);
    assert.equal(state.durationLabel, null);
  }
});

test('worker execution bounds retain at most 32 records per chat with truthful omissions', () => {
  const identity = makeIdentity('bnd_alpha_execution_bounds', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_bounded',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'Attempts are reconciled individually.',
    current_execution_ref: 'exec-32',
  });
  const executions = Array.from({ length: MAX_CHAT_WORK_EXECUTIONS + 3 }, (_, index) =>
    makeExecution(identity.binding_id, {
      item_id: 'blg_bounded',
      execution_ref: `exec-${index + 1}`,
      worker_status: 'failed',
      outcome: 'failure',
      started_at_ms: 1_000 + index,
      finished_at_ms: 1_100 + index,
      duration_ms: 100,
    }),
  );
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [item],
        events: [
          makeBacklogEvent({
            event_id: 'blg_bounded:1',
            sequence: 1,
            item_id: 'blg_bounded',
            revision: 1,
            execution_ref: 'exec-32',
            status: 'blocked',
            recorded_at_ms: 1_200,
          }),
        ],
        worker_executions: executions,
        bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 0 },
      }),
    ],
    identity,
  );
  assert.equal(selection.executions.length, MAX_CHAT_WORK_EXECUTIONS);
  assert.equal(selection.bounds.executionsOmitted, 3);
  assert.equal(selection.executions[0]!.execution_ref, 'exec-4');
  assert.equal(selection.executions.at(-1)!.execution_ref, 'exec-35');
});

test('execution duration preserves terminal values and only verified Working presence advances elapsed time', () => {
  const identity = makeIdentity('bnd_alpha_execution_duration', UUID_ALPHA);
  const durationAt = (
    nowMs: number,
    itemOverrides: Partial<ChatWorkItem>,
    executionOverrides: ChatWorkExecutionFixtureOverrides,
  ) => {
    const { summary } = selectExecutionFixture(identity, {
      item: itemOverrides,
      execution: executionOverrides,
    });
    return resolveChatWorkExecutionFooterState(summary, nowMs).durationLabel;
  };

  assert.equal(
    durationAt(
      CHAT_WORK_NOW_MS,
      { item_id: 'blg_done_duration', status: 'done', current_execution_ref: 'exec-1' },
      {
        worker_status: 'completed',
        outcome: 'success',
        started_at_ms: 1_000,
        finished_at_ms: 5_200,
        duration_ms: 4_200,
      },
    ),
    '4.2s',
  );
  assert.equal(
    durationAt(
      CHAT_WORK_NOW_MS,
      { item_id: 'blg_derived_duration', status: 'done', current_execution_ref: 'exec-1' },
      {
        worker_status: 'completed',
        outcome: 'success',
        started_at_ms: 1_000,
        finished_at_ms: 3_500,
        duration_ms: -1,
      },
    ),
    '2.5s',
  );
  assert.equal(
    durationAt(
      CHAT_WORK_NOW_MS,
      { item_id: 'blg_zero_duration', status: 'done', current_execution_ref: 'exec-1' },
      {
        worker_status: 'completed',
        outcome: 'success',
        started_at_ms: 1_000,
        finished_at_ms: 1_000,
        duration_ms: 0,
      },
    ),
    '0ms',
  );

  // A running record alone is only a dispatch receipt. It must not tick.
  assert.equal(
    durationAt(
      CHAT_WORK_NOW_MS,
      { item_id: 'blg_running_duration', current_execution_ref: 'exec-1' },
      {
        worker_status: 'running',
        outcome: null,
        started_at_ms: 9_000,
        finished_at_ms: null,
        duration_ms: null,
      },
    ),
    null,
  );
  assert.equal(
    durationAt(
      12_000,
      { item_id: 'blg_running_duration_later', current_execution_ref: 'exec-1' },
      {
        worker_status: 'running',
        outcome: null,
        started_at_ms: 9_000,
      },
    ),
    null,
  );

  const working = selectExecutionFixture(identity, {
    item: { item_id: 'blg_working_duration', current_execution_ref: 'exec-1' },
    execution: {
      worker_status: 'running',
      outcome: null,
      started_at_ms: 9_000,
    },
    presence: [
      makePresence({ item_id: 'blg_working_duration', execution_ref: 'exec-1' }),
    ],
  });
  assert.equal(
    resolveChatWorkExecutionFooterState(working.summary, CHAT_WORK_NOW_MS).statusKey,
    'working',
  );
  assert.equal(
    resolveChatWorkExecutionFooterState(working.summary, CHAT_WORK_NOW_MS).durationLabel,
    'Elapsed 1.5s',
  );
  assert.equal(
    resolveChatWorkExecutionFooterState(working.summary, 12_000).durationLabel,
    'Elapsed 3s',
  );

  assert.equal(
    durationAt(
      CHAT_WORK_NOW_MS,
      { item_id: 'blg_future_duration', current_execution_ref: 'exec-1' },
      {
        worker_status: 'running',
        outcome: null,
        started_at_ms: CHAT_WORK_NOW_MS + 5_000,
      },
    ),
    null,
  );
  assert.equal(
    durationAt(
      CHAT_WORK_NOW_MS,
      { item_id: 'blg_missing_duration', current_execution_ref: 'exec-1' },
      {
        worker_status: 'running',
        outcome: null,
        started_at_ms: null,
        finished_at_ms: null,
        duration_ms: null,
      },
    ),
    null,
  );
  assert.equal(
    durationAt(
      CHAT_WORK_NOW_MS,
      { item_id: 'blg_invalid_duration', status: 'done', current_execution_ref: 'exec-1' },
      {
        worker_status: 'completed',
        outcome: 'success',
        started_at_ms: 2_000,
        finished_at_ms: 1_000,
        duration_ms: -5,
      },
    ),
    null,
  );

  // Clock ticks only re-derive the Working label; the durable event view and its
  // sample/event timestamps remain unchanged, so scroll anchors are unaffected.
  const { selection, views, summary } = selectExecutionFixture(identity, {
    item: { item_id: 'blg_clock_duration', current_execution_ref: 'exec-1' },
    execution: {
      worker_status: 'running',
      outcome: null,
      started_at_ms: 9_000,
    },
    presence: [
      makePresence({ item_id: 'blg_clock_duration', execution_ref: 'exec-1' }),
    ],
  });
  const firstLabel = resolveChatWorkExecutionFooterState(
    summary,
    CHAT_WORK_NOW_MS,
  ).durationLabel;
  const firstViews = buildChatWorkEventViews(selection);
  const secondLabel = resolveChatWorkExecutionFooterState(
    summary,
    CHAT_WORK_NOW_MS + 2_000,
  ).durationLabel;
  assert.equal(firstLabel, 'Elapsed 1.5s');
  assert.equal(secondLabel, 'Elapsed 3.5s');
  assert.deepEqual(buildChatWorkEventViews(selection), firstViews);
  assert.equal(views[0]!.recordedAtMs, 1_001);

  let state = syncChatWorkEventScroll(
    createMessageScrollState('wchat:execution-clock'),
    firstViews,
  ).state;
  const layout = buildSimulatedLayout(
    firstViews.map((view) => ({ item_id: view.eventId })),
    new Map(),
    60,
    8,
    12,
    12,
  );
  const readingScrollTop = 0;
  state = handleMessageViewportScroll(
    state,
    {
      scrollTop: readingScrollTop,
      clientHeight: 40,
      scrollHeight: layout.scrollHeight,
    },
    measureItemsFromContentOffsets(layout.layoutItems, readingScrollTop),
  );
  const resync = syncChatWorkEventScroll(state, firstViews);
  assert.equal(resync.action.kind, 'preserve_anchor');
});

test('worker footer shows Working only for exact fresh presence and removes the ticking timer when it expires', () => {
  const identity = makeIdentity('bnd_alpha_execution_ttl', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_alpha',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-1',
  });
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [item],
        events: [
          makeBacklogEvent({
            event_id: 'blg_alpha:3',
            sequence: 3,
            item_id: 'blg_alpha',
            revision: 3,
            status: 'doing',
            execution_ref: 'exec-1',
            recorded_at_ms: 1_300,
          }),
        ],
        worker_presence: [makePresence({ item_id: 'blg_alpha' })],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: 'blg_alpha',
            execution_ref: 'exec-1',
            worker_status: 'running',
            outcome: null,
            started_at_ms: 9_000,
          }),
        ],
      }),
    ],
    identity,
  );

  const fresh = evaluateChatWorkLiveness(selection, null, { nowMs: CHAT_WORK_NOW_MS });
  assert.equal(fresh.isWorking, true);
  assert.equal(fresh.agents[0]!.role, 'worker');

  const footer = buildChatWorkEventViews(selection)[0]!.workerExecution!;
  const freshFooter = resolveChatWorkExecutionFooterState(footer, CHAT_WORK_NOW_MS);
  assert.equal(freshFooter.statusKey, 'working');
  assert.equal(freshFooter.statusLabel, 'Started');
  assert.equal(freshFooter.tone, 'neutral');
  assert.equal(
    freshFooter.statusAriaLabel,
    'Started, live evidence fresh (last confirmed 500ms ago)',
  );
  assert.equal(freshFooter.statusTitle, freshFooter.statusAriaLabel);
  assert.equal(freshFooter.durationLabel, 'Elapsed 1.5s');
  assert.equal(
    freshFooter.durationAriaLabel,
    'Elapsed 1.5s, live evidence fresh (last confirmed 500ms ago)',
  );
  assert.equal(freshFooter.durationTitle, freshFooter.durationAriaLabel);

  const expiredAtMs = 10_000 + 5_000 + 1;
  const expired = evaluateChatWorkLiveness(selection, null, {
    nowMs: expiredAtMs,
  });
  assert.equal(expired.isWorking, false);
  assert.deepEqual(expired.agents, []);

  // The durable dispatch receipt remains, but expired presence cannot sustain
  // Working or keep an independent footer timer advancing.
  assert.equal(footer.statusKey, 'dispatched');
  const expiredFooter = resolveChatWorkExecutionFooterState(footer, expiredAtMs);
  assert.equal(expiredFooter.statusKey, 'unavailable');
  assert.equal(expiredFooter.statusLabel, 'Status unavailable');
  assert.equal(expiredFooter.duration, null);
  assert.equal(expiredFooter.durationLabel, null);
});

test('worker footer becomes unavailable for stale, unavailable, future, or non-current presence', () => {
  const identity = makeIdentity('bnd_alpha_execution_presence_failures', UUID_ALPHA);
  const footerAt = (presence: ChatWorkPresence | null, nowMs = CHAT_WORK_NOW_MS) => {
    const { summary } = selectExecutionFixture(identity, {
      item: { item_id: 'blg_presence_failure', current_execution_ref: 'exec-1' },
      execution: {
        worker_status: 'running',
        outcome: null,
        started_at_ms: 9_000,
      },
      presence: presence !== null ? [presence] : [],
    });
    return resolveChatWorkExecutionFooterState(summary, nowMs);
  };

  const fresh = footerAt(
    makePresence({ item_id: 'blg_presence_failure', execution_ref: 'exec-1' }),
  );
  assert.equal(fresh.statusKey, 'working');
  assert.equal(fresh.durationLabel, 'Elapsed 1.5s');

  for (const presence of [
    makePresence({
      item_id: 'blg_presence_failure',
      execution_ref: 'exec-1',
      freshness: 'stale',
    }),
    makePresence({
      item_id: 'blg_presence_failure',
      execution_ref: 'exec-1',
      availability: 'unavailable',
    }),
    makePresence({
      item_id: 'blg_presence_failure',
      execution_ref: 'exec-1',
      sampled_at_ms: CHAT_WORK_NOW_MS + 1,
    }),
    makePresence({
      item_id: 'blg_presence_failure',
      execution_ref: 'exec-1',
      expires_at_ms: null,
    }),
  ]) {
    const state = footerAt(presence);
    assert.equal(state.statusKey, 'unavailable');
    assert.equal(state.duration, null);
    assert.equal(state.durationLabel, null);
  }

  // A presence record for another attempt is never attached to this execution.
  const otherAttempt = footerAt(
    makePresence({
      item_id: 'blg_presence_failure',
      execution_ref: 'exec-other',
    }),
  );
  assert.equal(otherAttempt.statusKey, 'dispatched');
  assert.equal(otherAttempt.duration, null);
});

test('worker footers key by item id plus execution ref and retain evidenced history after item eviction', () => {
  const identity = makeIdentity('bnd_alpha_execution_composite', UUID_ALPHA);
  const sharedRef = 'exec-shared';
  const sharedItemA = makeBacklogItem({
    item_id: 'blg_shared_a',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: sharedRef,
  });
  const sharedItemB = makeBacklogItem({
    item_id: 'blg_shared_b',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'A different item reuses the same textual ref.',
    current_execution_ref: sharedRef,
  });
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [sharedItemA, sharedItemB],
        events: [
          makeBacklogEvent({
            event_id: 'blg_shared_a:2',
            sequence: 2,
            item_id: 'blg_shared_a',
            revision: 2,
            action: 'worker_outcome_recorded',
            status: 'doing',
            execution_ref: sharedRef,
            outcome: 'failure',
            recorded_at_ms: 1_200,
          }),
          makeBacklogEvent({
            event_id: 'blg_shared_b:3',
            sequence: 3,
            item_id: 'blg_shared_b',
            revision: 3,
            action: 'worker_outcome_recorded',
            status: 'blocked',
            execution_ref: sharedRef,
            outcome: null,
            recorded_at_ms: 1_300,
          }),
          makeBacklogEvent({
            event_id: 'blg_evicted:4',
            sequence: 4,
            item_id: 'blg_evicted',
            revision: 4,
            action: 'worker_outcome_recorded',
            status: 'blocked',
            execution_ref: 'exec-historical',
            outcome: 'failure',
            recorded_at_ms: 1_400,
          }),
        ],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: 'blg_shared_a',
            execution_ref: sharedRef,
            worker_status: 'failed',
            outcome: 'failure',
            started_at_ms: 1_000,
            finished_at_ms: 1_200,
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_shared_b',
            execution_ref: sharedRef,
            worker_status: 'cancelled',
            outcome: null,
            started_at_ms: 1_100,
            finished_at_ms: 1_300,
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_evicted',
            execution_ref: 'exec-historical',
            session_binding_id: 'bnd_runtime_approved',
            worker_status: 'failed',
            outcome: 'failure',
            started_at_ms: 1_250,
            finished_at_ms: 1_400,
          }),
          makeExecution(identity.binding_id, {
            item_id: 'blg_without_event',
            execution_ref: 'exec-without-event',
            worker_status: 'failed',
            outcome: 'failure',
          }),
        ],
      }),
    ],
    identity,
  );

  assert.deepEqual(
    selection.executions.map((execution) => [execution.item_id, execution.execution_ref]),
    [
      ['blg_shared_a', sharedRef],
      ['blg_shared_b', sharedRef],
      ['blg_evicted', 'exec-historical'],
    ],
  );
  assert.equal(selection.bounds.executionsOmitted, 1);

  const views = buildChatWorkEventViews(selection);
  const footerByItem = new Map(
    views
      .filter((view) => view.workerExecution !== null)
      .map((view) => [view.itemId, view.workerExecution!] as const),
  );
  assert.equal(footerByItem.get('blg_shared_a')?.statusKey, 'failed');
  assert.equal(footerByItem.get('blg_shared_b')?.statusKey, 'cancelled');
  assert.equal(footerByItem.get('blg_evicted')?.statusKey, 'failed');
  assert.equal(footerByItem.get('blg_evicted')?.itemId, 'blg_evicted');
  assert.equal(
    selection.executions.find((execution) => execution.item_id === 'blg_evicted')
      ?.session_binding_id,
    'bnd_runtime_approved',
  );

  const evictedFooter = footerByItem.get('blg_evicted')!;
  const evictedState = resolveChatWorkExecutionFooterState(
    evictedFooter,
    CHAT_WORK_NOW_MS,
  );
  assert.equal(evictedState.statusKey, 'failed');
  assert.equal(evictedState.durationLabel, '150ms');
});

test('retained accepted execution keeps guarded Done after its item leaves the item window', () => {
  const identity = makeIdentity('bnd_alpha_execution_retained_done', UUID_ALPHA);
  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [],
        events: [
          makeBacklogEvent({
            event_id: 'blg_retained_done:5',
            sequence: 5,
            item_id: 'blg_retained_done',
            revision: 5,
            action: 'completed',
            status: 'done',
            execution_ref: 'exec-retained-done',
            recorded_at_ms: 1_500,
          }),
        ],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: 'blg_retained_done',
            execution_ref: 'exec-retained-done',
            session_binding_id: 'bnd_runtime_approved',
            worker_status: 'completed',
            outcome: 'success',
            acceptance: 'accepted',
            started_at_ms: 1_000,
            finished_at_ms: 1_450,
            duration_ms: 450,
          }),
        ],
      }),
    ],
    identity,
  );

  assert.deepEqual(selection.items, []);
  assert.equal(selection.executions.length, 1);
  const footer = buildChatWorkEventViews(selection)[0]!.workerExecution!;
  assert.equal(footer.itemStatus, null);
  const state = resolveChatWorkExecutionFooterState(footer, CHAT_WORK_NOW_MS);
  assert.equal(state.statusKey, 'completed');
  assert.equal(state.statusLabel, 'Completed');
  assert.equal(state.durationLabel, '450ms');
});

test('raw duplicate chat_work rows reconcile item and presence identities before their bounded caps', () => {
  const identity = makeIdentity('bnd_alpha_raw_duplicate_bounds', UUID_ALPHA);
  const items = Array.from({ length: 11 }, (_, index) =>
    makeBacklogItem({
      item_id: `blg_raw_duplicate_${index}`,
      binding_id: identity.binding_id,
      kind: 'worker',
      status: 'doing',
      current_execution_ref: `exec-raw-${index}`,
    }),
  );
  const firstItem = items[0]!;
  const secondItem = items[1]!;
  const repeatedPresence = Array.from({ length: 32 }, () =>
    makePresence({
      item_id: firstItem.item_id,
      execution_ref: firstItem.current_execution_ref!,
    }),
  );
  const independentPresence = makePresence({
    item_id: secondItem.item_id,
    execution_ref: secondItem.current_execution_ref!,
  });
  const work = makeChatWork(identity, {
    items,
    worker_presence: [...repeatedPresence, independentPresence],
  });

  const duplicated = selectWorkspaceChatWork([work, work], identity);
  assert.equal(duplicated.items.length, 11);
  assert.equal(new Set(duplicated.items.map((item) => item.item_id)).size, 11);
  assert.equal(duplicated.bounds.itemsOmitted, 11);

  const single = selectWorkspaceChatWork([work], identity);
  assert.deepEqual(
    single.presence.map((presence) => [presence.item_id, presence.execution_ref]),
    [
      [firstItem.item_id, firstItem.current_execution_ref],
      [secondItem.item_id, secondItem.current_execution_ref],
    ],
  );
  assert.equal(single.bounds.presenceOmitted, 31);
  assert.equal(
    evaluateChatWorkLiveness(single, null, { nowMs: CHAT_WORK_NOW_MS })
      .workingAgentCount,
    2,
  );

  const indexEntry = buildChatWorkLivenessIndex([work, work], [], {
    nowMs: CHAT_WORK_NOW_MS,
  }).get(resolveMessageScrollChatKey(identity))!;
  assert.equal(indexEntry.workingAgentCount, 2);
  assert.deepEqual(
    indexEntry.presence.map((presence) => presence.item_id),
    [firstItem.item_id, secondItem.item_id],
  );
});

test('raw duplicate reconciliation keeps the newest item revision, ordering-first sample winner, and exact chats separate', () => {
  const alphaIdentity = makeIdentity('bnd_alpha_raw_revision', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_beta_raw_revision', UUID_BETA);
  const attempt = {
    item_id: 'blg_raw_revision',
    execution_ref: 'exec-raw-revision',
  };
  const staleOlder = makePresence({
    ...attempt,
    sequence: 3,
    freshness: 'stale',
    sampled_at_ms: 9_600,
    sample_age_ms: 900,
  });
  const freshNewer = makePresence({
    ...attempt,
    sequence: 4,
    freshness: 'fresh',
    sampled_at_ms: 10_000,
    sample_age_ms: 500,
  });
  const alphaItems = [
    makeBacklogItem({
      item_id: attempt.item_id,
      binding_id: alphaIdentity.binding_id,
      kind: 'worker',
      status: 'todo',
      revision: 1,
      current_execution_ref: null,
    }),
    makeBacklogItem({
      item_id: attempt.item_id,
      binding_id: alphaIdentity.binding_id,
      kind: 'worker',
      status: 'doing',
      revision: 2,
      current_execution_ref: attempt.execution_ref,
    }),
  ];
  const alphaWork = makeChatWork(alphaIdentity, {
    items: alphaItems,
    worker_presence: [staleOlder, freshNewer],
  });
  const betaWork = makeChatWork(betaIdentity, {
    items: [
      makeBacklogItem({
        item_id: attempt.item_id,
        binding_id: betaIdentity.binding_id,
        kind: 'worker',
        status: 'doing',
        current_execution_ref: attempt.execution_ref,
      }),
    ],
    worker_presence: [makePresence(attempt)],
  });

  const alpha = selectWorkspaceChatWork([alphaWork, betaWork], alphaIdentity);
  assert.deepEqual(
    alpha.items.map((item) => [
      item.binding_id,
      item.item_id,
      item.revision,
      item.status,
    ]),
    [[alphaIdentity.binding_id, attempt.item_id, 2, 'doing']],
  );
  assert.equal(alpha.bounds.itemsOmitted, 1);
  assert.equal(alpha.presence.length, 1);
  assert.equal(alpha.presence[0]!.freshness, 'fresh');
  assert.equal(alpha.presence[0]!.sampled_at_ms, 10_000);
  assert.equal(alpha.bounds.presenceOmitted, 1);
  assert.equal(
    evaluateChatWorkLiveness(alpha, null, { nowMs: CHAT_WORK_NOW_MS })
      .workingAgentCount,
    1,
  );

  // On an exact ordering tie (same epoch, sequence, sampled_at_ms), fresh beats stale,
  // whereas a newer stale sample (larger sampled_at_ms at the same sequence) supersedes
  // the older fresh sample and clears Working.
  const staleTie = makePresence({
    ...attempt,
    sequence: 4,
    freshness: 'stale',
    sampled_at_ms: 10_000,
    sample_age_ms: 500,
  });
  const tiedAlpha = selectWorkspaceChatWork(
    [makeChatWork(alphaIdentity, { items: alphaItems, worker_presence: [staleTie, freshNewer] })],
    alphaIdentity,
  );
  assert.equal(tiedAlpha.presence[0]!.freshness, 'fresh');
  assert.equal(
    evaluateChatWorkLiveness(tiedAlpha, null, { nowMs: CHAT_WORK_NOW_MS }).workingAgentCount,
    1,
  );

  const staleNewer = makePresence({
    ...attempt,
    sequence: 4,
    freshness: 'stale',
    sampled_at_ms: 10_400,
    sample_age_ms: 100,
  });
  const degradedAlpha = selectWorkspaceChatWork(
    [makeChatWork(alphaIdentity, { items: alphaItems, worker_presence: [freshNewer, staleNewer] })],
    alphaIdentity,
  );
  assert.equal(degradedAlpha.presence.length, 1);
  assert.equal(degradedAlpha.presence[0]!.freshness, 'stale');
  assert.equal(degradedAlpha.presence[0]!.live, false);
  assert.equal(degradedAlpha.presence[0]!.sampled_at_ms, 10_400);
  assert.equal(
    evaluateChatWorkLiveness(degradedAlpha, null, { nowMs: CHAT_WORK_NOW_MS }).workingAgentCount,
    0,
  );

  const beta = selectWorkspaceChatWork([alphaWork, betaWork], betaIdentity);
  assert.deepEqual(
    beta.items.map((item) => [item.binding_id, item.item_id]),
    [[betaIdentity.binding_id, attempt.item_id]],
  );
  assert.equal(beta.bounds.itemsOmitted, 0);
  assert.equal(beta.presence.length, 1);
  assert.equal(beta.bounds.presenceOmitted, 0);
});

test('raw duplicate reconciliation never resurrects a quarantined execution identity', () => {
  const identity = makeIdentity('bnd_alpha_raw_quarantine', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_raw_quarantine',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'The attempt identity conflicts.',
    current_execution_ref: 'exec-raw-quarantine',
  });
  const event = makeBacklogEvent({
    event_id: 'blg_raw_quarantine:2',
    sequence: 2,
    item_id: item.item_id,
    revision: 2,
    action: 'worker_outcome_recorded',
    status: 'blocked',
    execution_ref: 'exec-raw-quarantine',
    outcome: 'failure',
    recorded_at_ms: 1_200,
  });
  const base = makeExecution(identity.binding_id, {
    item_id: item.item_id,
    execution_ref: 'exec-raw-quarantine',
    session_binding_id: 'bnd_runtime_raw_quarantine',
    worker_status: 'running',
    outcome: null,
    started_at_ms: 9_000,
  });
  const conflicting = makeExecution(identity.binding_id, {
    ...base,
    approval_id: 'approval-raw-other',
  });
  const terminal = makeExecution(identity.binding_id, {
    ...base,
    worker_status: 'completed',
    outcome: 'success',
    acceptance: 'accepted',
    finished_at_ms: 13_200,
    duration_ms: 4_200,
  });
  const conflictWork = makeChatWork(identity, {
    items: [item, item],
    events: [event, event],
    worker_presence: [
      makePresence({
        item_id: item.item_id,
        execution_ref: 'exec-raw-quarantine',
        approval_id: 'approval-raw-other',
      }),
      makePresence({
        item_id: item.item_id,
        execution_ref: 'exec-raw-quarantine',
      }),
    ],
    worker_executions: [base, conflicting],
  });
  const terminalWork = makeChatWork(identity, {
    items: [item],
    events: [event],
    worker_executions: [terminal],
  });

  const selection = selectWorkspaceChatWork([conflictWork, terminalWork], identity);
  assert.equal(selection.items.length, 1);
  assert.equal(selection.bounds.itemsOmitted, 2);
  assert.equal(selection.events.length, 1);
  assert.deepEqual(selection.executions, []);
  assert.equal(selection.bounds.executionsOmitted, 3);
  assert.equal(buildChatWorkEventViews(selection)[0]!.workerExecution, null);
  assert.equal(
    evaluateChatWorkLiveness(selection, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
    false,
  );

  const indexEntry = buildChatWorkLivenessIndex(
    [conflictWork, terminalWork],
    [makeNativeSample(identity, 'idle')],
    { nowMs: CHAT_WORK_NOW_MS },
  ).get(resolveMessageScrollChatKey(identity))!;
  assert.equal(indexEntry.isWorking, false);
  assert.equal(indexEntry.waitingForHuman, false);
});

test('passive technical-success pending acceptance renders as Awaiting check/neutral until matching guarded Done, rejection, or cancellation', () => {
  const identity = makeIdentity('bnd_alpha_awaiting_check', UUID_ALPHA);
  const itemId = 'blg_awaiting_check';
  const execRef = 'exec-awaiting-1';
  const reservedEvent = makeBacklogEvent({
    event_id: `${itemId}:2`,
    sequence: 2,
    item_id: itemId,
    revision: 2,
    action: 'worker_reserved',
    status: 'doing',
    execution_ref: execRef,
    recorded_at_ms: 1_100,
  });
  const attachedEvent = makeBacklogEvent({
    event_id: `${itemId}:3`,
    sequence: 3,
    item_id: itemId,
    revision: 3,
    action: 'worker_execution_attached',
    status: 'doing',
    execution_ref: execRef,
    recorded_at_ms: 1_200,
  });
  const outcomeEvent = makeBacklogEvent({
    event_id: `${itemId}:4`,
    sequence: 4,
    item_id: itemId,
    revision: 4,
    action: 'worker_outcome_recorded',
    status: 'doing',
    execution_ref: execRef,
    outcome: 'success',
    recorded_at_ms: 4_500,
  });

  for (const acceptance of ['pending_review', 'ready_for_review', null] as const) {
    const pendingSelection = selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [
            makeBacklogItem({
              item_id: itemId,
              binding_id: identity.binding_id,
              kind: 'worker',
              status: 'doing',
              revision: 4,
              current_execution_ref: execRef,
            }),
          ],
          events: [reservedEvent, attachedEvent, outcomeEvent],
          worker_presence: [
            makePresence({ item_id: itemId, execution_ref: execRef }),
          ],
          worker_executions: [
            makeExecution(identity.binding_id, {
              item_id: itemId,
              execution_ref: execRef,
              worker_status: 'completed',
              outcome: 'success',
              acceptance,
              started_at_ms: 1_000,
              finished_at_ms: 4_500,
              duration_ms: 3_500,
            }),
          ],
        }),
      ],
      identity,
    );

    const pendingViews = buildChatWorkEventViews(pendingSelection);
    assert.equal(pendingViews.length, 3);
    assert.equal(pendingViews[0]!.workerExecution, null);
    assert.equal(pendingViews[1]!.workerExecution, null);
    const pendingSummary = pendingViews[2]!.workerExecution!;
    assert.equal(pendingSummary.statusKey, 'checking');
    assert.equal(pendingSummary.statusLabel, 'Awaiting check');
    assert.equal(pendingSummary.tone, 'neutral');
    assert.deepEqual(pendingSummary.duration, { kind: 'final', durationMs: 3_500 });

    // Passive awaiting-check keeps terminal duration and never turns into active Working.
    for (const nowMs of [CHAT_WORK_NOW_MS, CHAT_WORK_NOW_MS + 60_000]) {
      const footer = resolveChatWorkExecutionFooterState(pendingSummary, nowMs);
      assert.equal(footer.statusKey, 'checking');
      assert.equal(footer.statusLabel, 'Awaiting check');
      assert.equal(footer.tone, 'neutral');
      assert.equal(footer.durationLabel, '3.5s');
    }
  }

  // Transition to matching guarded Done updates only the current footer while preserving immutable history.
  const completedEvent = makeBacklogEvent({
    event_id: `${itemId}:5`,
    sequence: 5,
    item_id: itemId,
    revision: 5,
    action: 'completed',
    status: 'done',
    execution_ref: execRef,
    recorded_at_ms: 5_000,
  });
  const doneSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [
          makeBacklogItem({
            item_id: itemId,
            binding_id: identity.binding_id,
            kind: 'worker',
            status: 'done',
            revision: 5,
            current_execution_ref: execRef,
            completion_note: 'Verified delivery and acceptance condition.',
            completion_evidence: 'SHA-256 matched expected artifact.',
            completed_at_ms: 5_000,
          }),
        ],
        events: [reservedEvent, attachedEvent, outcomeEvent, completedEvent],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: itemId,
            execution_ref: execRef,
            worker_status: 'completed',
            outcome: 'success',
            acceptance: 'accepted',
            started_at_ms: 1_000,
            finished_at_ms: 4_500,
            duration_ms: 3_500,
          }),
        ],
      }),
    ],
    identity,
  );
  const doneViews = buildChatWorkEventViews(doneSelection);
  assert.deepEqual(
    doneViews.map((view) => [
      view.eventId,
      view.actionLabel,
      view.recordedAtMs,
      view.outcome,
      view.workerExecution !== null,
    ]),
    [
      [`${itemId}:2`, 'Preparing', 1_100, null, false],
      [`${itemId}:3`, 'Worker dispatched', 1_200, null, false],
      [`${itemId}:4`, 'Awaiting check', 4_500, 'success', false],
      [`${itemId}:5`, 'Done', 5_000, null, true],
    ],
  );
  const doneFooter = resolveChatWorkExecutionFooterState(
    doneViews[3]!.workerExecution!,
    CHAT_WORK_NOW_MS,
  );
  assert.equal(doneFooter.statusKey, 'completed');
  assert.equal(doneFooter.statusLabel, 'Completed');
  assert.equal(doneFooter.tone, 'success');
  assert.equal(doneFooter.durationLabel, '3.5s');

  // Rejection and cancellation never auto-promote to Completed even if item status is marked done.
  const rejectedSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [
          makeBacklogItem({
            item_id: itemId,
            binding_id: identity.binding_id,
            kind: 'worker',
            status: 'done',
            revision: 5,
            current_execution_ref: execRef,
          }),
        ],
        events: [outcomeEvent],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: itemId,
            execution_ref: execRef,
            worker_status: 'completed',
            outcome: 'success',
            acceptance: 'rejected',
            started_at_ms: 1_000,
            finished_at_ms: 3_200,
            duration_ms: 2_200,
          }),
        ],
      }),
    ],
    identity,
  );
  const rejectedFooter = resolveChatWorkExecutionFooterState(
    buildChatWorkEventViews(rejectedSelection)[0]!.workerExecution!,
    CHAT_WORK_NOW_MS,
  );
  assert.equal(rejectedFooter.statusKey, 'rejected');
  assert.equal(rejectedFooter.statusLabel, 'Rejected');
  assert.equal(rejectedFooter.tone, 'danger');
  assert.equal(rejectedFooter.durationLabel, '2.2s');

  const cancelledSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [
          makeBacklogItem({
            item_id: itemId,
            binding_id: identity.binding_id,
            kind: 'worker',
            status: 'done',
            revision: 5,
            current_execution_ref: execRef,
          }),
        ],
        events: [outcomeEvent],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: itemId,
            execution_ref: execRef,
            worker_status: 'cancelled',
            outcome: null,
            acceptance: null,
            started_at_ms: 1_000,
            finished_at_ms: 2_800,
            duration_ms: 1_800,
          }),
        ],
      }),
    ],
    identity,
  );
  const cancelledFooter = resolveChatWorkExecutionFooterState(
    buildChatWorkEventViews(cancelledSelection)[0]!.workerExecution!,
    CHAT_WORK_NOW_MS,
  );
  assert.equal(cancelledFooter.statusKey, 'cancelled');
  assert.equal(cancelledFooter.statusLabel, 'Cancelled');
  assert.equal(cancelledFooter.tone, 'neutral');
  assert.equal(cancelledFooter.durationLabel, '1.8s');
});

test('mismatched execution ref never auto-completes on item Done and keeps human-input/liveness separation', () => {
  const identity = makeIdentity('bnd_alpha_mismatch_guard', UUID_ALPHA);
  const item = makeBacklogItem({
    item_id: 'blg_mismatch_guard',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'done',
    revision: 6,
    current_execution_ref: 'exec-current',
    completion_note: 'Accepted current attempt.',
    completion_evidence: 'Verified current execution.',
    completed_at_ms: 6_000,
  });
  const blockedItem = makeBacklogItem({
    item_id: 'blg_blocked_passive',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    revision: 2,
    blocked_reason: 'Acceptance failed on earlier attempt.',
    current_execution_ref: 'exec-blocked',
  });
  const doingItem = makeBacklogItem({
    item_id: 'blg_doing_no_presence',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    revision: 3,
    current_execution_ref: 'exec-doing-no-presence',
  });

  const selection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [item, blockedItem, doingItem],
        events: [
          makeBacklogEvent({
            event_id: 'blg_mismatch_guard:3',
            sequence: 3,
            item_id: item.item_id,
            revision: 3,
            action: 'worker_outcome_recorded',
            status: 'doing',
            execution_ref: 'exec-prior',
            outcome: 'success',
            recorded_at_ms: 2_500,
          }),
          makeBacklogEvent({
            event_id: 'blg_mismatch_guard:6',
            sequence: 6,
            item_id: item.item_id,
            revision: 6,
            action: 'completed',
            status: 'done',
            execution_ref: 'exec-current',
            recorded_at_ms: 6_000,
          }),
        ],
        worker_executions: [
          makeExecution(identity.binding_id, {
            item_id: item.item_id,
            execution_ref: 'exec-prior',
            worker_status: 'completed',
            outcome: 'success',
            acceptance: 'pending_review',
            started_at_ms: 1_000,
            finished_at_ms: 2_500,
            duration_ms: 1_500,
          }),
          makeExecution(identity.binding_id, {
            item_id: item.item_id,
            execution_ref: 'exec-current',
            worker_status: 'completed',
            outcome: 'success',
            acceptance: 'accepted',
            started_at_ms: 4_000,
            finished_at_ms: 5_900,
            duration_ms: 1_900,
          }),
        ],
      }),
    ],
    identity,
  );

  const views = buildChatWorkEventViews(selection);
  const priorFooter = resolveChatWorkExecutionFooterState(
    views[0]!.workerExecution!,
    CHAT_WORK_NOW_MS,
  );
  assert.equal(priorFooter.statusKey, 'checking');
  assert.equal(priorFooter.statusLabel, 'Awaiting check');
  assert.equal(priorFooter.tone, 'neutral');
  assert.equal(priorFooter.durationLabel, '1.5s');

  const currentFooter = resolveChatWorkExecutionFooterState(
    views[1]!.workerExecution!,
    CHAT_WORK_NOW_MS,
  );
  assert.equal(currentFooter.statusKey, 'completed');
  assert.equal(currentFooter.statusLabel, 'Completed');
  assert.equal(currentFooter.tone, 'success');
  assert.equal(currentFooter.durationLabel, '1.9s');

  // Doing without fresh Worker presence and Blocked/awaiting-check never infer Working or NeedsYou.
  const withoutSample = evaluateChatWorkLiveness(selection, null, {
    nowMs: CHAT_WORK_NOW_MS,
  });
  assert.equal(withoutSample.isWorking, false);
  assert.equal(withoutSample.workingAgentCount, 0);
  assert.equal(withoutSample.waitingForHuman, false);
  assert.equal(withoutSample.statusLabel, null);
  assert.equal(withoutSample.statusTone, 'neutral');

  const idleSample = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(idleSample.isWorking, false);
  assert.equal(idleSample.waitingForHuman, false);
  assert.equal(idleSample.statusLabel, 'Codex chat');
  assert.equal(idleSample.statusTone, 'neutral');

  // Only a fresh native waiting sample produces NeedsYou; stale samples clear it.
  const freshWaiting = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'waiting_for_input'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(freshWaiting.waitingForHuman, true);
  assert.equal(freshWaiting.isWorking, false);
  assert.equal(freshWaiting.statusLabel, 'Waiting for you in Codex');
  assert.equal(freshWaiting.statusTone, 'attention');

  const staleWaiting = evaluateChatWorkLiveness(
    selection,
    makeNativeSample(identity, 'waiting_for_input'),
    { nowMs: CHAT_WORK_NOW_MS + 60_000 },
  );
  assert.equal(staleWaiting.waitingForHuman, false);
  assert.equal(staleWaiting.isWorking, false);
});

test('newer explicit non_live clears Working over a still-fresh older live sample and inverse ordering works in both input orders', () => {
  const identity = makeIdentity('bnd_alpha_newest_stop', UUID_ALPHA);
  const itemId = 'blg_newest_stop';
  const execRef = 'exec-newest-stop';
  const doingItem = makeBacklogItem({
    item_id: itemId,
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    revision: 3,
    current_execution_ref: execRef,
    title: 'Run live evidence selection',
  });
  const dispatchEvent = makeBacklogEvent({
    event_id: `${itemId}:3`,
    sequence: 3,
    item_id: itemId,
    revision: 3,
    action: 'worker_execution_attached',
    status: 'doing',
    execution_ref: execRef,
    recorded_at_ms: 9_100,
  });
  const execution = makeExecution(identity.binding_id, {
    item_id: itemId,
    execution_ref: execRef,
    worker_status: 'running',
    outcome: null,
    started_at_ms: 9_000,
  });

  const liveSeq4 = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    sequence: 4,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_000,
    sample_age_ms: 100,
    max_sample_age_ms: 5_000,
    expires_at_ms: 15_000,
  });
  const nonLiveSeq5 = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    sequence: 5,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
    sample_age_ms: 50,
    max_sample_age_ms: 5_000,
    expires_at_ms: 15_200,
  });

  // Same canonical binding/item/execution/attempt/epoch: sequence 5 explicit non_live
  // clears Working even while sequence 4 live is still fresh at CHAT_WORK_NOW_MS (10_500).
  const singleEntryOrders: ReadonlyArray<readonly WorkspaceChatWork[]> = [
    [
      makeChatWork(identity, {
        items: [doingItem],
        events: [dispatchEvent],
        worker_executions: [execution],
        worker_presence: [liveSeq4, nonLiveSeq5],
      }),
    ],
    [
      makeChatWork(identity, {
        items: [doingItem],
        events: [dispatchEvent],
        worker_executions: [execution],
        worker_presence: [nonLiveSeq5, liveSeq4],
      }),
    ],
    // Mixed-source split across two raw chat_work entries for the same canonical identity.
    [
      makeChatWork(identity, {
        items: [doingItem],
        events: [dispatchEvent],
        worker_executions: [execution],
        worker_presence: [liveSeq4],
      }),
      makeChatWork(identity, {
        items: [],
        events: [],
        worker_executions: [],
        worker_presence: [nonLiveSeq5],
      }),
    ],
  ];

  for (const entries of singleEntryOrders) {
    const selection = selectWorkspaceChatWork(entries, identity);
    assert.equal(selection.presence.length, 1);
    assert.equal(selection.presence[0]!.availability, 'non_live');
    assert.equal(selection.presence[0]!.sequence, 5);
    assert.equal(selection.presence[0]!.live, false);
    assert.equal(selection.bounds.presenceOmitted, 1);

    const liveness = evaluateChatWorkLiveness(selection, null, {
      nowMs: CHAT_WORK_NOW_MS,
    });
    assert.equal(liveness.isWorking, false);
    assert.equal(liveness.workingAgentCount, 0);
    assert.deepEqual(liveness.agents, []);
    assert.equal(liveness.statusLabel, null);

    const indexEntry = buildChatWorkLivenessIndex(entries, [], {
      nowMs: CHAT_WORK_NOW_MS,
    }).get(resolveMessageScrollChatKey(identity))!;
    assert.equal(indexEntry.isWorking, false);
    assert.equal(indexEntry.workingAgentCount, 0);

    const footer = buildChatWorkEventViews(selection)[0]!.workerExecution!;
    const footerState = resolveChatWorkExecutionFooterState(footer, CHAT_WORK_NOW_MS);
    assert.equal(footerState.statusKey, 'unavailable');
    assert.equal(footerState.statusLabel, 'Status unavailable');
    assert.equal(footerState.duration, null);
    assert.equal(footerState.durationLabel, null);
  }

  // Caller-constructed unreconciled selection also clears Working when seq 5 non_live is present.
  const baseLiveSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingItem],
        events: [dispatchEvent],
        worker_executions: [execution],
        worker_presence: [liveSeq4],
      }),
    ],
    identity,
  );
  const manualUnreconciled = {
    ...baseLiveSelection,
    presence: [liveSeq4, nonLiveSeq5],
  };
  assert.equal(
    evaluateChatWorkLiveness(manualUnreconciled, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
    false,
  );
  const manualFooter = buildChatWorkEventViews(manualUnreconciled)[0]!.workerExecution!;
  assert.equal(
    resolveChatWorkExecutionFooterState(manualFooter, CHAT_WORK_NOW_MS).statusKey,
    'unavailable',
  );

  // Inverse ordering: older non_live or unavailable (seq 4) loses to newer live (seq 5),
  // showing Working until wall-clock freshness expires.
  const nonLiveSeq4 = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    sequence: 4,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_000,
  });
  const unavailableSeq4 = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    sequence: 4,
    availability: 'unavailable',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_000,
  });
  const liveSeq5 = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    sequence: 5,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_200,
    sample_age_ms: 50,
    max_sample_age_ms: 5_000,
    expires_at_ms: 15_200,
  });

  for (const presenceRows of [
    [nonLiveSeq4, liveSeq5],
    [liveSeq5, nonLiveSeq4],
    [unavailableSeq4, liveSeq5],
    [liveSeq5, unavailableSeq4],
  ]) {
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [doingItem],
          events: [dispatchEvent],
          worker_executions: [execution],
          worker_presence: presenceRows,
        }),
      ],
      identity,
    );
    assert.equal(selection.presence.length, 1);
    assert.equal(selection.presence[0]!.availability, 'live');
    assert.equal(selection.presence[0]!.sequence, 5);
    assert.equal(selection.presence[0]!.live, true);

    const liveEval = evaluateChatWorkLiveness(selection, null, {
      nowMs: CHAT_WORK_NOW_MS,
    });
    assert.equal(liveEval.isWorking, true);
    assert.equal(liveEval.workingAgentCount, 1);
    assert.equal(liveEval.statusLabel, 'Working');

    const footer = buildChatWorkEventViews(selection)[0]!.workerExecution!;
    const freshFooter = resolveChatWorkExecutionFooterState(footer, CHAT_WORK_NOW_MS);
    assert.equal(freshFooter.statusKey, 'working');
    assert.equal(freshFooter.durationLabel, 'Elapsed 1.5s');

    // Freshness TTL is preserved on the winning live sample.
    const expiredAtMs = 15_201;
    assert.equal(
      evaluateChatWorkLiveness(selection, null, { nowMs: expiredAtMs }).isWorking,
      false,
    );
    const expiredFooter = resolveChatWorkExecutionFooterState(footer, expiredAtMs);
    assert.equal(expiredFooter.statusKey, 'unavailable');
    assert.equal(expiredFooter.durationLabel, null);
  }
});

test('restarted epoch reset sequence, unknown observation, separate attempts, and stale/malformed isolation stay accurate end-to-end', () => {
  const alphaIdentity = makeIdentity('bnd_alpha_epoch_isolation', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_beta_epoch_isolation', UUID_BETA);
  const itemId = 'blg_epoch_isolation';
  const execRef = 'exec-epoch-1';
  const doingItem = makeBacklogItem({
    item_id: itemId,
    binding_id: alphaIdentity.binding_id,
    kind: 'worker',
    status: 'doing',
    revision: 4,
    current_execution_ref: execRef,
  });
  const dispatchEvent = makeBacklogEvent({
    event_id: `${itemId}:4`,
    sequence: 4,
    item_id: itemId,
    revision: 4,
    action: 'worker_execution_attached',
    status: 'doing',
    execution_ref: execRef,
    recorded_at_ms: 9_100,
  });
  const execution = makeExecution(alphaIdentity.binding_id, {
    item_id: itemId,
    execution_ref: execRef,
    worker_status: 'running',
    outcome: null,
    started_at_ms: 9_000,
  });

  const oldEpochLive = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    host_instance_id: 'host-1',
    invocation_id: 'invocation-1',
    claim_id: 'claim-1',
    sequence: 4,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_000,
    expires_at_ms: 15_000,
  });
  const restartedEpochStop = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    host_instance_id: 'host-2',
    invocation_id: 'invocation-2',
    claim_id: 'claim-2',
    sequence: 1,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_300,
    expires_at_ms: 15_300,
  });
  const olderEpochHighSeq = makePresence({
    item_id: itemId,
    execution_ref: execRef,
    host_instance_id: 'host-0',
    invocation_id: 'invocation-0',
    claim_id: 'claim-0',
    sequence: 99,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 9_000,
    expires_at_ms: 14_000,
  });

  // 1. Restarted epoch with reset sequence=1 at newer sampled_at_ms clears Working.
  for (const rows of [
    [oldEpochLive, olderEpochHighSeq, restartedEpochStop],
    [restartedEpochStop, olderEpochHighSeq, oldEpochLive],
  ]) {
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(alphaIdentity, {
          items: [doingItem],
          events: [dispatchEvent],
          worker_executions: [execution],
          worker_presence: rows,
        }),
      ],
      alphaIdentity,
    );
    assert.equal(selection.presence.length, 1);
    assert.equal(selection.presence[0]!.availability, 'non_live');
    assert.equal(selection.presence[0]!.sequence, 1);
    assert.equal(
      evaluateChatWorkLiveness(selection, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
      false,
    );
  }

  // Restarted epoch live with reset sequence=1 supersedes old epoch stop at older timestamp.
  const oldEpochStop = makePresence({
    ...oldEpochLive,
    sequence: 5,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
  });
  const restartedEpochLive = makePresence({
    ...restartedEpochStop,
    availability: 'live',
    freshness: 'fresh',
    live: true,
    sampled_at_ms: 10_400,
    expires_at_ms: 15_400,
  });
  for (const rows of [
    [oldEpochStop, restartedEpochLive],
    [restartedEpochLive, oldEpochStop],
  ]) {
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(alphaIdentity, {
          items: [doingItem],
          events: [dispatchEvent],
          worker_executions: [execution],
          worker_presence: rows,
        }),
      ],
      alphaIdentity,
    );
    assert.equal(selection.presence[0]!.sequence, 1);
    assert.equal(selection.presence[0]!.live, true);
    assert.equal(
      evaluateChatWorkLiveness(selection, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
      true,
    );
  }

  // 2. Unknown (unavailable) observation neither falsely asserts active (Working) nor idle.
  const newerUnknown = makePresence({
    ...oldEpochLive,
    sequence: 5,
    availability: 'unavailable',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
    expires_at_ms: 15_200,
  });
  for (const rows of [
    [newerUnknown],
    [oldEpochLive, newerUnknown],
    [newerUnknown, oldEpochLive],
  ]) {
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(alphaIdentity, {
          items: [doingItem],
          events: [dispatchEvent],
          worker_executions: [execution],
          worker_presence: rows,
        }),
      ],
      alphaIdentity,
    );
    assert.equal(selection.presence[0]!.availability, 'unavailable');
    assert.equal(selection.presence[0]!.live, false);
    const liveness = evaluateChatWorkLiveness(selection, null, {
      nowMs: CHAT_WORK_NOW_MS,
    });
    assert.equal(liveness.isWorking, false);
    assert.equal(liveness.workingAgentCount, 0);
    assert.equal(liveness.statusLabel, null);
    assert.equal(liveness.statusTone, 'neutral');
    const footer = buildChatWorkEventViews(selection)[0]!.workerExecution!;
    const footerState = resolveChatWorkExecutionFooterState(footer, CHAT_WORK_NOW_MS);
    assert.equal(footerState.statusKey, 'unavailable');
    assert.equal(footerState.statusLabel, 'Status unavailable');
    assert.equal(footerState.durationLabel, null);
  }

  // 3. Separate attempts stay isolated (stop on foreign attempt never clears active attempt;
  // foreign live attempt never marks a stopped verified execution as Working).
  const foreignAttemptStop = makePresence({
    ...oldEpochLive,
    run_id: 'run-foreign',
    sequence: 9,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_400,
  });
  const attemptIsolationSelection = selectWorkspaceChatWork(
    [
      makeChatWork(alphaIdentity, {
        items: [doingItem],
        events: [dispatchEvent],
        worker_executions: [execution],
        worker_presence: [oldEpochLive, foreignAttemptStop],
      }),
    ],
    alphaIdentity,
  );
  assert.equal(attemptIsolationSelection.presence.length, 2);
  assert.equal(
    evaluateChatWorkLiveness(attemptIsolationSelection, null, { nowMs: CHAT_WORK_NOW_MS })
      .workingAgentCount,
    1,
  );
  const isolatedFooter = buildChatWorkEventViews(attemptIsolationSelection)[0]!.workerExecution!;
  assert.equal(
    resolveChatWorkExecutionFooterState(isolatedFooter, CHAT_WORK_NOW_MS).statusKey,
    'working',
  );

  const foreignAttemptLive = makePresence({
    ...oldEpochLive,
    worker_id: 'worker-foreign',
    sequence: 9,
    sampled_at_ms: 10_400,
    expires_at_ms: 15_400,
  });
  const activeAttemptStop = makePresence({
    ...oldEpochLive,
    sequence: 5,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
  });
  const verifiedAttemptSelection = selectWorkspaceChatWork(
    [
      makeChatWork(alphaIdentity, {
        items: [doingItem],
        events: [dispatchEvent],
        worker_executions: [execution],
        worker_presence: [activeAttemptStop, foreignAttemptLive],
      }),
    ],
    alphaIdentity,
  );
  assert.equal(
    evaluateChatWorkLiveness(verifiedAttemptSelection, null, { nowMs: CHAT_WORK_NOW_MS })
      .isWorking,
    false,
  );

  // 4. Cross-execution, cross-chat, malformed, and deterministic tie isolation.
  const secondItem = makeBacklogItem({
    item_id: 'blg_epoch_other_item',
    binding_id: alphaIdentity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-epoch-2',
  });
  const secondItemLive = makePresence({
    item_id: secondItem.item_id,
    execution_ref: secondItem.current_execution_ref!,
    sequence: 2,
    sampled_at_ms: 10_000,
    expires_at_ms: 15_000,
  });
  const betaItem = makeBacklogItem({
    item_id: itemId,
    binding_id: betaIdentity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: execRef,
  });
  const malformedRows: unknown[] = [
    makePresence({ ...activeAttemptStop, approval_id: '' }),
    { ...activeAttemptStop, availability: 'bogus_status' },
    { ...activeAttemptStop, freshness: 'bogus_freshness' },
    { ...activeAttemptStop, sequence: -1 },
    { ...activeAttemptStop, sampled_at_ms: -5 },
    { ...activeAttemptStop, live: 'false' },
  ];
  const alphaMalformedSelection = selectWorkspaceChatWork(
    [
      makeChatWork(alphaIdentity, {
        items: [doingItem, secondItem],
        worker_presence: [
          oldEpochLive,
          secondItemLive,
          ...(malformedRows as ChatWorkPresence[]),
        ],
      }),
      makeChatWork(betaIdentity, {
        items: [betaItem],
        worker_presence: [activeAttemptStop],
      }),
    ],
    alphaIdentity,
  );
  assert.equal(alphaMalformedSelection.presence.length, 2);
  assert.equal(alphaMalformedSelection.bounds.presenceOmitted, malformedRows.length);
  assert.equal(
    evaluateChatWorkLiveness(alphaMalformedSelection, null, { nowMs: CHAT_WORK_NOW_MS })
      .workingAgentCount,
    2,
  );

  // Exact ordering tie (same epoch, sequence 4, sampled_at_ms 10_000): explicit non_live
  // deterministically beats live in either input order.
  const exactTieStop = makePresence({
    ...oldEpochLive,
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
  });
  for (const rows of [
    [oldEpochLive, exactTieStop],
    [exactTieStop, oldEpochLive],
  ]) {
    const tied = selectWorkspaceChatWork(
      [makeChatWork(alphaIdentity, { items: [doingItem], worker_presence: rows })],
      alphaIdentity,
    );
    assert.equal(tied.presence[0]!.availability, 'non_live');
    assert.equal(tied.presence[0]!.live, false);
    assert.equal(
      evaluateChatWorkLiveness(tied, null, { nowMs: CHAT_WORK_NOW_MS }).isWorking,
      false,
    );
  }
});

test('resolveSidebarChatStatus distinguishes confirmed idle and explicit terminal samples from missing, unknown, stale, disconnected, and suppressed observations', () => {
  const identity = makeIdentity('bnd_sidebar_status', UUID_ALPHA);
  const todoItem = makeBacklogItem({
    item_id: 'blg_todo',
    binding_id: identity.binding_id,
    kind: 'coworker',
    status: 'todo',
  });
  const doingWorkerItem = makeBacklogItem({
    item_id: 'blg_doing_worker',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-doing-1',
  });
  const blockedFailedWorkerItem = makeBacklogItem({
    item_id: 'blg_failed_worker',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'blocked',
    blocked_reason: 'Worker failed without requesting human input.',
    current_execution_ref: 'exec-failed-1',
  });
  const nonLiveDoingWorkerPresence = makePresence({
    item_id: doingWorkerItem.item_id,
    execution_ref: 'exec-doing-1',
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
    sample_age_ms: null,
    expires_at_ms: null,
  });
  const nonLiveTerminalPresence = makePresence({
    item_id: blockedFailedWorkerItem.item_id,
    execution_ref: 'exec-failed-1',
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 9_800,
    sample_age_ms: null,
    expires_at_ms: null,
  });

  // Absent liveness entry never claims No live agent or warns the human.
  assert.deepEqual(resolveSidebarChatStatus(undefined), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });
  assert.deepEqual(resolveSidebarChatStatus(null), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });

  // Missing native sample with passive backlog items (Todo/Doing/Blocked) stays unavailable.
  const passiveSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [todoItem, doingWorkerItem, blockedFailedWorkerItem],
      }),
    ],
    identity,
  );
  const missingSampleLiveness = evaluateChatWorkLiveness(passiveSelection, null, {
    nowMs: CHAT_WORK_NOW_MS,
  });
  assert.deepEqual(resolveSidebarChatStatus(missingSampleLiveness), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });

  // Partial observation: fresh idle Coworker with an unconfirmed Doing Worker
  // (missing or omitted worker_presence) stays neutral Status unavailable
  // without fabricating a live agent or Needs you.
  const partialIdle = evaluateChatWorkLiveness(
    passiveSelection,
    makeNativeSample(identity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(partialIdle.isWorking, false);
  assert.equal(partialIdle.workingAgentCount, 0);
  assert.equal(partialIdle.waitingForHuman, false);
  assert.deepEqual(resolveSidebarChatStatus(partialIdle), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });

  const omittedWorkerPresenceSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingWorkerItem],
        worker_presence: [],
        bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 1 },
      }),
    ],
    identity,
  );
  const omittedWorkerIdle = evaluateChatWorkLiveness(
    omittedWorkerPresenceSelection,
    makeNativeSample(identity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(omittedWorkerIdle.isWorking, false);
  assert.equal(omittedWorkerIdle.workingAgentCount, 0);
  assert.equal(omittedWorkerIdle.waitingForHuman, false);
  assert.deepEqual(resolveSidebarChatStatus(omittedWorkerIdle), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });

  // Complete observation: fresh idle native sample renders Idle once there is
  // no outstanding Doing Worker or every relevant Doing Worker is explicitly non_live.
  const completeIdleSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [todoItem, doingWorkerItem, blockedFailedWorkerItem],
        worker_presence: [nonLiveDoingWorkerPresence],
      }),
    ],
    identity,
  );
  const confirmedIdle = evaluateChatWorkLiveness(
    completeIdleSelection,
    makeNativeSample(identity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.deepEqual(resolveSidebarChatStatus(confirmedIdle), {
    kind: 'idle',
    label: 'Idle',
    tone: 'neutral',
  });

  const idleNoOutstandingSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [todoItem, blockedFailedWorkerItem],
      }),
    ],
    identity,
  );
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        idleNoOutstandingSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'idle',
      label: 'Idle',
      tone: 'neutral',
    },
  );

  // Fresh native sample with unknown activity is NOT confirmed idle.
  const unknownActivity = evaluateChatWorkLiveness(
    completeIdleSelection,
    makeNativeSample(identity, 'unknown'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.deepEqual(resolveSidebarChatStatus(unknownActivity), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });

  // Stale, disconnected, and unavailable native samples stay Status unavailable
  // even if a blocked worker carries a terminal non_live presence record.
  const terminalOnlySelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [todoItem, blockedFailedWorkerItem],
        worker_presence: [nonLiveTerminalPresence],
      }),
    ],
    identity,
  );
  for (const degradedLiveness of [
    evaluateChatWorkLiveness(
      terminalOnlySelection,
      makeNativeSample(identity, 'idle'),
      { nowMs: CHAT_WORK_NOW_MS + 60_000 },
    ),
    evaluateChatWorkLiveness(
      terminalOnlySelection,
      makeNativeSample(identity, 'idle', 10_000, 'disconnected'),
      { nowMs: CHAT_WORK_NOW_MS },
    ),
    evaluateChatWorkLiveness(
      terminalOnlySelection,
      makeNativeSample(identity, 'idle', 10_000, 'unavailable'),
      { nowMs: CHAT_WORK_NOW_MS },
    ),
  ]) {
    assert.deepEqual(resolveSidebarChatStatus(degradedLiveness), {
      kind: 'unknown',
      label: 'Status unavailable',
      tone: 'neutral',
    });
  }

  // Current explicit terminal worker presence (non_live, no contradicting native
  // sample, no unconfirmed Doing worker) renders No live agent.
  const terminalLiveness = evaluateChatWorkLiveness(terminalOnlySelection, null, {
    nowMs: CHAT_WORK_NOW_MS,
  });
  assert.deepEqual(resolveSidebarChatStatus(terminalLiveness), {
    kind: 'terminal',
    label: 'No live agent',
    tone: 'neutral',
  });

  // Snapshot staleness or refresh failure suppresses terminal presence too.
  assert.deepEqual(resolveSidebarChatStatus(terminalLiveness, { suppressed: true }), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        completeIdleSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS, isStale: true },
      ),
    ),
    {
      kind: 'unknown',
      label: 'Status unavailable',
      tone: 'neutral',
    },
  );

  // Terminal presence on a blocked item must not mask an unconfirmed Doing worker
  // when no native sample exists.
  const mixedTerminalAndUnconfirmedDoing = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingWorkerItem, blockedFailedWorkerItem],
        worker_presence: [nonLiveTerminalPresence],
      }),
    ],
    identity,
  );
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(mixedTerminalAndUnconfirmedDoing, null, {
        nowMs: CHAT_WORK_NOW_MS,
      }),
    ),
    {
      kind: 'unknown',
      label: 'Status unavailable',
      tone: 'neutral',
    },
  );

  // An expired or stale worker presence on an active Doing item prevents
  // claiming Idle even when Coworker is fresh idle.
  const expiredWorkerSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingWorkerItem],
        worker_presence: [
          makePresence({
            item_id: doingWorkerItem.item_id,
            execution_ref: 'exec-doing-1',
            expires_at_ms: CHAT_WORK_NOW_MS - 1,
          }),
        ],
      }),
    ],
    identity,
  );
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        expiredWorkerSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'unknown',
      label: 'Status unavailable',
      tone: 'neutral',
    },
  );

  // Existing waiting and working presentations are preserved verbatim.
  const waitingLiveness = evaluateChatWorkLiveness(
    passiveSelection,
    makeNativeSample(identity, 'waiting_for_approval'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.deepEqual(resolveSidebarChatStatus(waitingLiveness), {
    kind: 'waiting',
    label: 'Waiting for you in Codex',
    tone: 'attention',
  });

  const workingLiveness = evaluateChatWorkLiveness(
    passiveSelection,
    makeNativeSample(identity, 'working'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.deepEqual(resolveSidebarChatStatus(workingLiveness), {
    kind: 'working',
    label: 'Working',
    tone: 'working',
  });
});

test('resolveSidebarChatStatus stays Status unavailable when one of two Doing Workers is missing its sample and keeps live precedence', () => {
  const identity = makeIdentity('bnd_sidebar_two_workers', UUID_ALPHA);
  const workerA = makeBacklogItem({
    item_id: 'blg_two_worker_a',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-two-a',
  });
  const workerB = makeBacklogItem({
    item_id: 'blg_two_worker_b',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-two-b',
  });
  const nonLiveA = makePresence({
    item_id: workerA.item_id,
    execution_ref: 'exec-two-a',
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
    sample_age_ms: null,
    expires_at_ms: null,
  });
  const nonLiveB = makePresence({
    item_id: workerB.item_id,
    execution_ref: 'exec-two-b',
    worker_id: 'worker-2',
    run_id: 'run-2',
    task_id: 'task-2',
    availability: 'non_live',
    freshness: 'not_applicable',
    live: false,
    sampled_at_ms: 10_200,
    sample_age_ms: null,
    expires_at_ms: null,
  });

  const missingSecondSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [workerA, workerB],
        worker_presence: [nonLiveA],
        bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 1 },
      }),
    ],
    identity,
  );
  const missingSecondLiveness = evaluateChatWorkLiveness(
    missingSecondSelection,
    makeNativeSample(identity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(missingSecondLiveness.workingAgentCount, 0);
  assert.equal(missingSecondLiveness.isWorking, false);
  assert.equal(missingSecondLiveness.waitingForHuman, false);
  assert.deepEqual(resolveSidebarChatStatus(missingSecondLiveness), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });

  // Confirmed live work on Worker A still renders Working with count 1 when Worker B is omitted.
  const oneLiveOneMissingSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [workerA, workerB],
        worker_presence: [
          makePresence({ item_id: workerA.item_id, execution_ref: 'exec-two-a' }),
        ],
        bounds: { items_omitted: 0, events_omitted: 0, presence_omitted: 1 },
      }),
    ],
    identity,
  );
  const oneLiveOneMissingLiveness = evaluateChatWorkLiveness(
    oneLiveOneMissingSelection,
    makeNativeSample(identity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(oneLiveOneMissingLiveness.workingAgentCount, 1);
  assert.equal(oneLiveOneMissingLiveness.isWorking, true);
  assert.deepEqual(resolveSidebarChatStatus(oneLiveOneMissingLiveness), {
    kind: 'working',
    label: 'Working',
    tone: 'working',
  });

  // Once both relevant Doing Workers are explicitly non_live, fresh idle Coworker renders Idle.
  const bothNonLiveSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [workerA, workerB],
        worker_presence: [nonLiveA, nonLiveB],
      }),
    ],
    identity,
  );
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        bothNonLiveSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'idle',
      label: 'Idle',
      tone: 'neutral',
    },
  );
});

test('resolveSidebarChatStatus rejects mismatched previous-attempt non_live evidence for Idle', () => {
  const identity = makeIdentity('bnd_sidebar_prev_attempt', UUID_ALPHA);
  const doingWorker = makeBacklogItem({
    item_id: 'blg_attempt_worker',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-attempt-2',
  });
  const currentExecution = makeExecution(identity.binding_id, {
    item_id: doingWorker.item_id,
    execution_ref: 'exec-attempt-2',
    run_id: 'run-attempt-2',
    worker_id: 'worker-attempt-2',
  });

  // Explicit non_live for an earlier execution_ref does not confirm the current execution.
  const previousRefSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingWorker],
        worker_executions: [currentExecution],
        worker_presence: [
          makePresence({
            item_id: doingWorker.item_id,
            execution_ref: 'exec-attempt-1',
            run_id: 'run-attempt-1',
            worker_id: 'worker-attempt-1',
            availability: 'non_live',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: 10_200,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
    ],
    identity,
  );
  const previousRefLiveness = evaluateChatWorkLiveness(
    previousRefSelection,
    makeNativeSample(identity, 'idle'),
    { nowMs: CHAT_WORK_NOW_MS },
  );
  assert.equal(previousRefLiveness.workingAgentCount, 0);
  assert.equal(previousRefLiveness.isWorking, false);
  assert.equal(previousRefLiveness.waitingForHuman, false);
  assert.deepEqual(resolveSidebarChatStatus(previousRefLiveness), {
    kind: 'unknown',
    label: 'Status unavailable',
    tone: 'neutral',
  });

  // Explicit non_live with the same execution_ref but mismatched attempt run_id also stays unavailable.
  const wrongRunSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingWorker],
        worker_executions: [currentExecution],
        worker_presence: [
          makePresence({
            item_id: doingWorker.item_id,
            execution_ref: 'exec-attempt-2',
            run_id: 'run-attempt-1',
            worker_id: 'worker-attempt-2',
            availability: 'non_live',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: 10_200,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
    ],
    identity,
  );
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        wrongRunSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'unknown',
      label: 'Status unavailable',
      tone: 'neutral',
    },
  );

  // Matching current attempt non_live confirms Idle.
  const matchingAttemptSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [doingWorker],
        worker_executions: [currentExecution],
        worker_presence: [
          makePresence({
            item_id: doingWorker.item_id,
            execution_ref: 'exec-attempt-2',
            run_id: 'run-attempt-2',
            worker_id: 'worker-attempt-2',
            availability: 'non_live',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: 10_200,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
    ],
    identity,
  );
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        matchingAttemptSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'idle',
      label: 'Idle',
      tone: 'neutral',
    },
  );
});

test('resolveSidebarChatStatus renders Idle when Coworker is fresh idle with no outstanding execution or reconciled non_live Workers', () => {
  const identity = makeIdentity('bnd_sidebar_confirmed_idle', UUID_ALPHA);

  // Just idle Coworker with no chat_work recorded yet renders Idle.
  const coworkerOnlySelection = selectWorkspaceChatWork([], identity);
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        coworkerOnlySelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'idle',
      label: 'Idle',
      tone: 'neutral',
    },
  );

  // Idle Coworker with no attached current execution renders Idle.
  const noOutstandingSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [
          makeBacklogItem({
            item_id: 'blg_coworker_doing',
            binding_id: identity.binding_id,
            kind: 'coworker',
            status: 'doing',
          }),
          makeBacklogItem({
            item_id: 'blg_worker_unattached',
            binding_id: identity.binding_id,
            kind: 'worker',
            status: 'doing',
            current_execution_ref: null,
          }),
          makeBacklogItem({
            item_id: 'blg_worker_done',
            binding_id: identity.binding_id,
            kind: 'worker',
            status: 'done',
            current_execution_ref: 'exec-done-1',
          }),
        ],
      }),
    ],
    identity,
  );
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        noOutstandingSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'idle',
      label: 'Idle',
      tone: 'neutral',
    },
  );

  // Reconciled newer explicit non_live superseding an older live sample confirms Idle
  // even though deduplication increments bounds.presenceOmitted.
  const reconciledSelection = selectWorkspaceChatWork(
    [
      makeChatWork(identity, {
        items: [
          makeBacklogItem({
            item_id: 'blg_worker_reconciled',
            binding_id: identity.binding_id,
            kind: 'worker',
            status: 'doing',
            current_execution_ref: 'exec-reconciled-1',
          }),
        ],
        worker_presence: [
          makePresence({
            item_id: 'blg_worker_reconciled',
            execution_ref: 'exec-reconciled-1',
            sequence: 4,
            availability: 'live',
            freshness: 'fresh',
            live: true,
            sampled_at_ms: 10_000,
            expires_at_ms: 15_000,
          }),
          makePresence({
            item_id: 'blg_worker_reconciled',
            execution_ref: 'exec-reconciled-1',
            sequence: 5,
            availability: 'non_live',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: 10_200,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
    ],
    identity,
  );
  assert.equal(reconciledSelection.bounds.presenceOmitted, 1);
  assert.deepEqual(
    resolveSidebarChatStatus(
      evaluateChatWorkLiveness(
        reconciledSelection,
        makeNativeSample(identity, 'idle'),
        { nowMs: CHAT_WORK_NOW_MS },
      ),
    ),
    {
      kind: 'idle',
      label: 'Idle',
      tone: 'neutral',
    },
  );
});

test('item truncation prevents unproven Idle and terminal labels without hiding live activity', () => {
  const identity = makeIdentity('bnd_sidebar_truncated_items', UUID_ALPHA);
  const completedItems = Array.from({ length: MAX_CHAT_WORK_ITEMS }, (_, index) =>
    makeBacklogItem({
      item_id: `blg_done_${index}`,
      binding_id: identity.binding_id,
      kind: 'worker',
      status: 'done',
      revision: 2,
      created_at_ms: 2_000 + index,
      updated_at_ms: 2_100 + index,
      completed_at_ms: 2_100 + index,
    }),
  );
  const olderWorker = makeBacklogItem({
    item_id: 'blg_older_doing_worker',
    binding_id: identity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-older-doing',
    created_at_ms: 1_000,
    updated_at_ms: 1_500,
  });
  const itemBounds = { items_omitted: 1, events_omitted: 0, presence_omitted: 0 };
  const eventBounds = { ...itemBounds, items_omitted: 0, events_omitted: 3 };
  const selection = (work: Partial<WorkspaceChatWork>) =>
    selectWorkspaceChatWork([makeChatWork(identity, work)], identity);
  const observe = (
    page: ReturnType<typeof selectWorkspaceChatWork>,
    activity: Parameters<typeof makeNativeSample>[1] | null,
  ) => evaluateChatWorkLiveness(page, activity === null ? null : makeNativeSample(identity, activity), {
    nowMs: CHAT_WORK_NOW_MS,
  });
  const unavailable = { kind: 'unknown', label: 'Status unavailable', tone: 'neutral' };
  const idle = { kind: 'idle', label: 'Idle', tone: 'neutral' };
  const working = { kind: 'working', label: 'Working', tone: 'working' };

  const truncated = selection({ items: [...completedItems, olderWorker] });
  const fromBackend = selection({ items: completedItems, bounds: itemBounds });
  for (const page of [truncated, fromBackend]) {
    assert.equal(page.items.length, MAX_CHAT_WORK_ITEMS);
    assert.ok(page.items.every((item) => item.status === 'done'));
    assert.equal(page.bounds.itemsOmitted, 1);
    const current = observe(page, 'idle');
    assert.equal(current.isWorking, false);
    assert.equal(current.workingAgentCount, 0);
    assert.equal(current.waitingForHuman, false);
    assert.deepEqual(resolveSidebarChatStatus(current), unavailable);
  }

  const blocked = makeBacklogItem({
    item_id: 'blg_visible_blocked', binding_id: identity.binding_id,
    kind: 'worker', status: 'blocked', current_execution_ref: 'exec-visible-blocked',
    blocked_reason: 'Worker exited without requesting input.',
  });
  const visibleWorkers = [olderWorker, blocked];
  const terminalPresence = visibleWorkers.map((item) => makePresence({
    item_id: item.item_id, execution_ref: item.current_execution_ref!,
    availability: 'non_live', freshness: 'not_applicable', live: false,
    sampled_at_ms: 10_200, sample_age_ms: null, expires_at_ms: null,
  }));
  const visibleNonLive = { items: visibleWorkers, worker_presence: terminalPresence };
  const incomplete = selection({ ...visibleNonLive, bounds: itemBounds });
  assert.deepEqual(resolveSidebarChatStatus(observe(incomplete, 'idle')), unavailable);
  assert.deepEqual(resolveSidebarChatStatus(observe(incomplete, null)), unavailable);

  assert.deepEqual(resolveSidebarChatStatus(observe(fromBackend, 'working')), working);
  assert.deepEqual(resolveSidebarChatStatus(observe(fromBackend, 'waiting_for_approval')), {
    kind: 'waiting', label: 'Waiting for you in Codex', tone: 'attention',
  });
  const withLiveWorker = selection({
    items: [olderWorker, ...completedItems.slice(1)],
    worker_presence: [makePresence({
      item_id: olderWorker.item_id, execution_ref: olderWorker.current_execution_ref!,
    })],
    bounds: itemBounds,
  });
  const live = observe(withLiveWorker, 'idle');
  assert.equal(live.isWorking, true);
  assert.equal(live.workingAgentCount, 1);
  assert.deepEqual(resolveSidebarChatStatus(live), working);

  // Stream history can be incomplete independently of the current item list.
  const events = Array.from({ length: MAX_CHAT_WORK_EVENTS + 1 }, (_, index) =>
    makeBacklogEvent({
      event_id: `blg_done_0:${index + 1}`, sequence: index + 1,
      item_id: 'blg_done_0', revision: index + 1, action: 'completed',
      status: 'done', recorded_at_ms: 2_000 + index,
    }),
  );
  const eventOverflow = selection({ items: completedItems, events });
  assert.equal(eventOverflow.bounds.itemsOmitted, 0);
  assert.equal(eventOverflow.bounds.eventsOmitted, 1);
  assert.deepEqual(resolveSidebarChatStatus(observe(eventOverflow, 'idle')), idle);

  const completeItems = selection({ ...visibleNonLive, bounds: eventBounds });
  assert.deepEqual(resolveSidebarChatStatus(observe(completeItems, 'idle')), idle);
  assert.deepEqual(resolveSidebarChatStatus(observe(completeItems, null)), {
    kind: 'terminal', label: 'No live agent', tone: 'neutral',
  });
});

test('sidebar navigation counters separate working agents from waiting chats with singular and plural labels', () => {
  assert.equal(formatSidebarWorkingCounter(0), null);
  assert.equal(formatSidebarWorkingCounter(-2), null);
  assert.equal(formatSidebarWorkingCounter(Number.NaN), null);
  assert.deepEqual(formatSidebarWorkingCounter(1), {
    count: 1,
    label: '1 working agent',
    ariaLabel: '1 working agent',
    tone: 'working',
  });
  assert.deepEqual(formatSidebarWorkingCounter(2), {
    count: 2,
    label: '2 working agents',
    ariaLabel: '2 working agents',
    tone: 'working',
  });

  assert.equal(formatSidebarWaitingCounter(0), null);
  assert.equal(formatSidebarWaitingCounter(-1), null);
  assert.deepEqual(formatSidebarWaitingCounter(1), {
    count: 1,
    label: '1 waiting chat',
    ariaLabel: '1 chat waiting for you',
    tone: 'attention',
  });
  assert.deepEqual(formatSidebarWaitingCounter(2), {
    count: 2,
    label: '2 waiting chats',
    ariaLabel: '2 chats waiting for you',
    tone: 'attention',
  });

  assert.equal(formatSidebarLifecycleWaitingLabel('active', 0), null);
  assert.equal(
    formatSidebarLifecycleWaitingLabel('active', 1),
    '1 active chat waiting for you',
  );
  assert.equal(
    formatSidebarLifecycleWaitingLabel('active', 2),
    '2 active chats waiting for you',
  );
  assert.equal(
    formatSidebarLifecycleWaitingLabel('archived', 1),
    '1 archived chat waiting for you',
  );
  assert.equal(
    formatSidebarLifecycleWaitingLabel('archived', 3),
    '3 archived chats waiting for you',
  );

  // One chat with two working agents (Coworker + Worker) plus one separate
  // waiting chat keeps working agents (2) and waiting chats (1) distinct.
  const alphaIdentity = makeIdentity('bnd_nav_alpha', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_nav_beta', UUID_BETA);
  const workerA = makeBacklogItem({
    item_id: 'blg_worker_a',
    binding_id: alphaIdentity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-a',
  });
  const workerB = makeBacklogItem({
    item_id: 'blg_worker_b',
    binding_id: alphaIdentity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-b',
  });

  const coworkerAndWorkerIndex = buildChatWorkLivenessIndex(
    [
      makeChatWork(alphaIdentity, {
        items: [workerA],
        worker_presence: [
          makePresence({ item_id: workerA.item_id, execution_ref: 'exec-a' }),
        ],
      }),
    ],
    [
      makeNativeSample(alphaIdentity, 'working'),
      makeNativeSample(betaIdentity, 'waiting_for_approval'),
    ],
    { nowMs: CHAT_WORK_NOW_MS },
  );
  const summary = summarizeChatWorkLiveness(coworkerAndWorkerIndex);
  assert.equal(summary.workingChatCount, 1);
  assert.equal(summary.workingAgentCount, 2);
  assert.equal(summary.waitingChatCount, 1);
  assert.equal(
    formatSidebarWorkingCounter(summary.workingAgentCount)?.label,
    '2 working agents',
  );
  assert.equal(
    formatSidebarWaitingCounter(summary.waitingChatCount)?.label,
    '1 waiting chat',
  );
  assert.equal(
    formatSidebarWaitingCounter(summary.waitingChatCount)?.ariaLabel,
    '1 chat waiting for you',
  );

  // One chat with two live Worker agents also counts 2 working agents without
  // fabricating any waiting chat.
  const twoWorkersIndex = buildChatWorkLivenessIndex(
    [
      makeChatWork(alphaIdentity, {
        items: [workerA, workerB],
        worker_presence: [
          makePresence({ item_id: workerA.item_id, execution_ref: 'exec-a' }),
          makePresence({
            item_id: workerB.item_id,
            execution_ref: 'exec-b',
            worker_id: 'worker-2',
            run_id: 'run-2',
            task_id: 'task-2',
          }),
        ],
      }),
    ],
    [makeNativeSample(alphaIdentity, 'idle')],
    { nowMs: CHAT_WORK_NOW_MS },
  );
  const twoWorkersSummary = summarizeChatWorkLiveness(twoWorkersIndex);
  assert.equal(twoWorkersSummary.workingChatCount, 1);
  assert.equal(twoWorkersSummary.workingAgentCount, 2);
  assert.equal(twoWorkersSummary.waitingChatCount, 0);
  assert.equal(
    formatSidebarWorkingCounter(twoWorkersSummary.workingAgentCount)?.label,
    '2 working agents',
  );
  assert.equal(
    formatSidebarWaitingCounter(twoWorkersSummary.waitingChatCount),
    null,
  );
});

test('refresh Worker presence reconciler retains only same-attempt same-origin newer samples without extending expiry', () => {
  const alphaIdentity = makeIdentity('bnd_alpha_reconcile', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_beta_reconcile', UUID_BETA);
  const baseLive = makePresence({
    item_id: 'blg_alpha',
    execution_ref: 'exec-1',
    sequence: 12,
    sampled_at_ms: 10_000,
    sample_age_ms: 500,
    max_sample_age_ms: 5_000,
    expires_at_ms: 15_000,
  });

  // Same attempt and trustworthy origin/epoch: regressed sequence and older
  // timestamp retain the newer sample verbatim.
  assert.equal(
    shouldRetainPreviousChatWorkPresence(
      baseLive,
      makePresence({
        ...baseLive,
        sequence: 11,
        sampled_at_ms: 8_000,
        expires_at_ms: 13_000,
      }),
    ),
    true,
  );
  assert.equal(
    shouldRetainPreviousChatWorkPresence(
      baseLive,
      makePresence({
        ...baseLive,
        sequence: 12,
        sampled_at_ms: 9_500,
        expires_at_ms: 14_500,
      }),
    ),
    true,
  );
  assert.equal(
    shouldRetainPreviousChatWorkPresence(
      baseLive,
      makePresence({
        ...baseLive,
        sequence: 12,
        freshness: 'stale',
        live: false,
      }),
    ),
    true,
  );

  // Explicit non_live at equal or newer sequence wins immediately.
  assert.equal(
    shouldRetainPreviousChatWorkPresence(
      baseLive,
      makePresence({
        ...baseLive,
        sequence: 12,
        availability: 'non_live',
        freshness: 'not_applicable',
        live: false,
      }),
    ),
    false,
  );
  assert.equal(
    shouldRetainPreviousChatWorkPresence(
      baseLive,
      makePresence({
        ...baseLive,
        sequence: 13,
        availability: 'unavailable',
        freshness: 'not_applicable',
        live: false,
        sampled_at_ms: 12_000,
        expires_at_ms: null,
      }),
    ),
    false,
  );

  // Restarted host, changed invocation/claim, missing origin, or foreign run
  // must never retain previous presence.
  for (const incoming of [
    makePresence({ ...baseLive, host_instance_id: 'host-2', sequence: 1 }),
    makePresence({ ...baseLive, invocation_id: 'invocation-2', sequence: 1 }),
    makePresence({ ...baseLive, claim_id: 'claim-2', sequence: 1 }),
    makePresence({ ...baseLive, host_instance_id: null, sequence: 1 }),
    makePresence({ ...baseLive, invocation_id: null, sequence: 1 }),
    makePresence({ ...baseLive, run_id: 'run-foreign', sequence: 1 }),
    makePresence({ ...baseLive, worker_id: 'worker-foreign', sequence: 1 }),
    makePresence({ ...baseLive, execution_ref: 'exec-foreign', sequence: 1 }),
  ]) {
    assert.equal(shouldRetainPreviousChatWorkPresence(baseLive, incoming), false);
  }

  // Entry-level reconciliation isolates chats, preserves original expiry, honors
  // terminal executions, and never resurrects omitted presence.
  const alphaItem = makeBacklogItem({
    item_id: 'blg_alpha',
    binding_id: alphaIdentity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-1',
  });
  const betaItem = makeBacklogItem({
    item_id: 'blg_alpha',
    binding_id: betaIdentity.binding_id,
    kind: 'worker',
    status: 'doing',
    current_execution_ref: 'exec-1',
  });
  const currentEntries = [
    makeChatWork(alphaIdentity, {
      items: [alphaItem],
      worker_presence: [baseLive],
      worker_executions: [makeExecution(alphaIdentity.binding_id)],
    }),
  ];
  const regressedEntries = [
    makeChatWork(alphaIdentity, {
      items: [alphaItem],
      worker_presence: [
        makePresence({
          ...baseLive,
          sequence: 11,
          sampled_at_ms: 8_000,
          expires_at_ms: 13_000,
        }),
      ],
      worker_executions: [makeExecution(alphaIdentity.binding_id)],
    }),
    makeChatWork(betaIdentity, {
      items: [betaItem],
      worker_presence: [
        makePresence({
          ...baseLive,
          sequence: 11,
          availability: 'unavailable',
          freshness: 'not_applicable',
          live: false,
          sampled_at_ms: 8_000,
          expires_at_ms: null,
        }),
      ],
      worker_executions: [makeExecution(betaIdentity.binding_id)],
    }),
  ];

  const reconciled = reconcileRefreshChatWorkPresenceEntries(
    currentEntries,
    regressedEntries,
  );
  assert.equal(reconciled.replaced, true);
  assert.equal(reconciled.chatWork[0]!.worker_presence[0]!.sequence, 12);
  assert.equal(reconciled.chatWork[0]!.worker_presence[0]!.expires_at_ms, 15_000);
  assert.equal(reconciled.chatWork[1]!.worker_presence[0]!.availability, 'unavailable');

  // Terminal execution immediately wins even if presence sequence regressed.
  const terminalIncoming = [
    makeChatWork(alphaIdentity, {
      items: [alphaItem],
      worker_presence: [
        makePresence({
          ...baseLive,
          sequence: 11,
          availability: 'unavailable',
          freshness: 'not_applicable',
          live: false,
          sampled_at_ms: 8_000,
          expires_at_ms: null,
        }),
      ],
      worker_executions: [
        makeExecution(alphaIdentity.binding_id, {
          worker_status: 'completed',
          outcome: 'success',
          acceptance: 'pending_review',
          finished_at_ms: 12_000,
          duration_ms: 3_000,
        }),
      ],
    }),
  ];
  const terminalReconciled = reconcileRefreshChatWorkPresenceEntries(
    currentEntries,
    terminalIncoming,
  );
  assert.equal(terminalReconciled.replaced, false);
  assert.equal(
    terminalReconciled.chatWork[0]!.worker_presence[0]!.availability,
    'unavailable',
  );

  // Omitted presence on refresh is never resurrected.
  const omittedIncoming = [
    makeChatWork(alphaIdentity, {
      items: [alphaItem],
      worker_presence: [],
      worker_executions: [makeExecution(alphaIdentity.binding_id)],
    }),
  ];
  const omittedReconciled = reconcileRefreshChatWorkPresenceEntries(
    currentEntries,
    omittedIncoming,
  );
  assert.equal(omittedReconciled.replaced, false);
  assert.deepEqual(omittedReconciled.chatWork[0]!.worker_presence, []);
});

test('worker dispatch footer holds durable Started with frozen last-confirmed elapsed across short repeated renewal gaps, expires genuinely, and preserves header/sidebar semantics', () => {
  const identity = makeIdentity('bnd_alpha_brief_gap', UUID_ALPHA);
  const chatKey = resolveMessageScrollChatKey(identity);
  const buildViews = (
    presenceRows: readonly ChatWorkPresence[],
    executionOverrides: ChatWorkExecutionFixtureOverrides = {},
    itemOverrides: Partial<ChatWorkItem> = {},
  ) => {
    const item = makeBacklogItem({
      item_id: 'blg_gap',
      binding_id: identity.binding_id,
      kind: 'worker',
      status: 'doing',
      current_execution_ref: 'exec-gap-1',
      ...itemOverrides,
    });
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [item],
          events: [
            makeBacklogEvent({
              event_id: 'blg_gap:3',
              sequence: 3,
              item_id: 'blg_gap',
              revision: 3,
              action: 'worker_execution_attached',
              status: 'doing',
              execution_ref: 'exec-gap-1',
              recorded_at_ms: 9_100,
            }),
          ],
          worker_presence: [...presenceRows],
          worker_executions: [
            makeExecution(identity.binding_id, {
              item_id: 'blg_gap',
              execution_ref: 'exec-gap-1',
              worker_status: 'running',
              outcome: null,
              started_at_ms: 9_000,
              ...executionOverrides,
            }),
          ],
        }),
      ],
      identity,
    );
    return {
      selection,
      events: buildChatWorkEventViews(selection),
    };
  };

  // Unconfirmed dispatch never claims Started before live confirmation.
  const unconfirmed = buildViews([]);
  const unconfirmedFooter = resolveChatWorkExecutionFooterState(
    unconfirmed.events[0]!.workerExecution!,
    10_000,
  );
  assert.equal(unconfirmedFooter.statusKey, 'dispatched');
  assert.equal(unconfirmedFooter.statusLabel, 'Dispatched');
  assert.equal(unconfirmedFooter.duration, null);
  assert.equal(unconfirmedFooter.durationLabel, null);
  assert.equal(unconfirmedFooter.statusAriaLabel, undefined);
  assert.equal(unconfirmedFooter.statusTitle, undefined);

  // Sample 1: live from 10_000 until 15_000 (brief gap window ends at 20_000).
  const liveSample1 = makePresence({
    item_id: 'blg_gap',
    execution_ref: 'exec-gap-1',
    sequence: 10,
    sampled_at_ms: 10_000,
    sample_age_ms: 500,
    max_sample_age_ms: 5_000,
    expires_at_ms: 15_000,
  });
  const live1 = buildViews([liveSample1]);
  const eventKey = live1.events[0]!.key;

  let store = createChatWorkExecutionContinuityStore();
  let step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    live1.events,
    14_000,
    { activeChatKey: chatKey },
  );
  store = step.store;
  const liveFooter1 = step.footersByEventKey.get(eventKey)!;
  assert.equal(liveFooter1.statusKey, 'working');
  assert.equal(liveFooter1.statusLabel, 'Started');
  assert.equal(liveFooter1.tone, 'neutral');
  assert.equal(
    liveFooter1.statusAriaLabel,
    'Started, live evidence fresh (last confirmed 4s ago)',
  );
  assert.equal(liveFooter1.statusTitle, liveFooter1.statusAriaLabel);
  assert.equal(liveFooter1.duration?.kind, 'elapsed');
  assert.equal(liveFooter1.durationLabel, 'Elapsed 5s');
  assert.equal(
    liveFooter1.durationAriaLabel,
    'Elapsed 5s, live evidence fresh (last confirmed 4s ago)',
  );
  assert.equal(liveFooter1.durationTitle, liveFooter1.durationAriaLabel);

  // Gap #1 at 15_500 (past original 15_000 expiry, inside 5_000ms brief gap):
  // header and sidebar IMMEDIATELY drop Working, while footer keeps consistent
  // neutral Started and frozen Elapsed 5s with accessible freshness/confirmation text.
  const gap1Liveness = evaluateChatWorkLiveness(
    live1.selection,
    makeNativeSample(identity, 'idle'),
    { nowMs: 15_500 },
  );
  assert.equal(gap1Liveness.isWorking, false);
  assert.equal(gap1Liveness.waitingForHuman, false);
  assert.deepEqual(gap1Liveness.agents, []);
  assert.equal(resolveSidebarChatStatus(gap1Liveness).kind, 'unknown');
  assert.equal(resolveSidebarChatStatus(gap1Liveness).label, 'Status unavailable');

  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    live1.events,
    15_500,
    { activeChatKey: chatKey },
  );
  store = step.store;
  const gap1Footer = step.footersByEventKey.get(eventKey)!;
  assert.equal(gap1Footer.statusKey, 'started');
  assert.equal(gap1Footer.statusLabel, 'Started');
  assert.equal(
    gap1Footer.statusAriaLabel,
    'Started, last confirmed status (confirmed 5.5s ago)',
  );
  assert.equal(gap1Footer.statusTitle, gap1Footer.statusAriaLabel);
  assert.equal(gap1Footer.tone, 'neutral');
  assert.deepEqual(gap1Footer.duration, { kind: 'last_confirmed', elapsedMs: 5_000 });
  assert.equal(gap1Footer.durationLabel, 'Elapsed 5s');
  assert.equal(
    gap1Footer.durationAriaLabel,
    'Last confirmed elapsed 5s (confirmed 5.5s ago)',
  );
  assert.equal(gap1Footer.durationTitle, gap1Footer.durationAriaLabel);

  // Delayed/stale and same-host unavailable probes during Gap #1 NEVER extend
  // original expiry, NEVER grant live Working, and NEVER advance elapsed.
  const staleDuringGap1 = buildViews([
    makePresence({
      ...liveSample1,
      sequence: 10,
      freshness: 'stale',
      live: false,
      sampled_at_ms: 10_000,
      expires_at_ms: 15_000,
    }),
  ]);
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    staleDuringGap1.events,
    17_500,
    { activeChatKey: chatKey },
  );
  store = step.store;
  const gap1StaleFooter = step.footersByEventKey.get(eventKey)!;
  assert.equal(gap1StaleFooter.statusKey, 'started');
  assert.equal(gap1StaleFooter.statusLabel, 'Started');
  assert.equal(gap1StaleFooter.durationLabel, 'Elapsed 5s');
  assert.equal(
    gap1StaleFooter.statusAriaLabel,
    'Started, last confirmed status (confirmed 7.5s ago)',
  );

  const sameHostUnavailable = buildViews([
    makePresence({
      ...liveSample1,
      invocation_id: null,
      claim_id: null,
      sequence: null,
      availability: 'unavailable',
      freshness: 'not_applicable',
      live: false,
      sampled_at_ms: 18_000,
      sample_age_ms: null,
      expires_at_ms: null,
    }),
  ]);
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    sameHostUnavailable.events,
    18_500,
    { activeChatKey: chatKey },
  );
  store = step.store;
  assert.equal(step.footersByEventKey.get(eventKey)!.statusKey, 'started');
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 5s');

  // Recovery #1: fresh sample arrives at 19_000 -> immediately restores live
  // internal state while keeping visible Started / Elapsed consistent.
  const liveSample2 = makePresence({
    item_id: 'blg_gap',
    execution_ref: 'exec-gap-1',
    sequence: 11,
    sampled_at_ms: 18_500,
    sample_age_ms: 500,
    max_sample_age_ms: 5_000,
    expires_at_ms: 23_500,
  });
  const live2 = buildViews([liveSample2]);
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    live2.events,
    19_000,
    { activeChatKey: chatKey },
  );
  store = step.store;
  assert.equal(step.footersByEventKey.get(eventKey)!.statusKey, 'working');
  assert.equal(step.footersByEventKey.get(eventKey)!.statusLabel, 'Started');
  assert.equal(step.footersByEventKey.get(eventKey)!.tone, 'neutral');
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 10s');
  assert.equal(
    step.footersByEventKey.get(eventKey)!.statusTitle,
    'Started, live evidence fresh (last confirmed 500ms ago)',
  );

  // Advance live tick to 23_000 (14s), then Gap #2 (brief omitted presence from
  // a timed-out host probe) holds updated frozen elapsed (14s) until recovery.
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    live2.events,
    23_000,
    { activeChatKey: chatKey },
  );
  store = step.store;
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 14s');

  const omittedDuringGap2 = buildViews([]);
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    omittedDuringGap2.events,
    24_500,
    { activeChatKey: chatKey },
  );
  store = step.store;
  const gap2Footer = step.footersByEventKey.get(eventKey)!;
  assert.equal(gap2Footer.statusKey, 'started');
  assert.equal(gap2Footer.statusLabel, 'Started');
  assert.equal(gap2Footer.durationLabel, 'Elapsed 14s');
  assert.equal(
    gap2Footer.durationAriaLabel,
    'Last confirmed elapsed 14s (confirmed 6s ago)',
  );

  // Recovery #2 with fresh seq 12 (expires at 30_000), then real expiration at
  // 30_000 + CHAT_WORK_EXECUTION_BRIEF_GAP_MS (35_000) -> Status unavailable
  // with duration removed.
  const liveSample3 = makePresence({
    item_id: 'blg_gap',
    execution_ref: 'exec-gap-1',
    sequence: 12,
    sampled_at_ms: 25_000,
    sample_age_ms: 500,
    max_sample_age_ms: 5_000,
    expires_at_ms: 30_000,
  });
  const live3 = buildViews([liveSample3]);
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    live3.events,
    26_000,
    { activeChatKey: chatKey },
  );
  store = step.store;
  assert.equal(step.footersByEventKey.get(eventKey)!.statusKey, 'working');
  assert.equal(step.footersByEventKey.get(eventKey)!.statusLabel, 'Started');
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 17s');

  const realExpiryMs = 30_000 + CHAT_WORK_EXECUTION_BRIEF_GAP_MS;
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    live3.events,
    realExpiryMs,
    { activeChatKey: chatKey },
  );
  store = step.store;
  const expiredFooter = step.footersByEventKey.get(eventKey)!;
  assert.equal(expiredFooter.statusKey, 'unavailable');
  assert.equal(expiredFooter.statusLabel, 'Status unavailable');
  assert.equal(expiredFooter.duration, null);
  assert.equal(expiredFooter.durationLabel, null);
  assert.equal(store.size, 1);
  assert.equal(
    store.get(live3.events[0]!.workerExecution!.attemptKey!)?.briefGapClosed,
    true,
  );
  // Subsequent empty worker_presence after real expiry stays Status unavailable
  // with no timer instead of reverting to Dispatched.
  step = resolveChatWorkExecutionFootersWithContinuity(
    store,
    buildViews([]).events,
    realExpiryMs + 1_000,
    { activeChatKey: chatKey },
  );
  store = step.store;
  const expiredEmptyFooter = step.footersByEventKey.get(eventKey)!;
  assert.equal(expiredEmptyFooter.statusKey, 'unavailable');
  assert.equal(expiredEmptyFooter.statusLabel, 'Status unavailable');
  assert.equal(expiredEmptyFooter.duration, null);
  assert.equal(expiredEmptyFooter.durationLabel, null);

  // Three successive 4-second late renewals with a 3-second TTL keep the same
  // visible neutral Started caption and Elapsed prefix without jumping backwards,
  // while exposing freshness and last-confirmed age via accessible text/title.
  let cadenceStore = createChatWorkExecutionContinuityStore();
  let previousFrozenMs = -1;
  for (let cycle = 0; cycle < 3; cycle += 1) {
    const sampledAtMs = 50_000 + cycle * 4_000;
    const expiresAtMs = sampledAtMs + 3_000;
    const sample = makePresence({
      item_id: 'blg_gap',
      execution_ref: 'exec-gap-1',
      sequence: 20 + cycle,
      sampled_at_ms: sampledAtMs,
      sample_age_ms: 0,
      max_sample_age_ms: 3_000,
      expires_at_ms: expiresAtMs,
    });
    const views = buildViews([sample], { started_at_ms: 45_000 });

    const liveStart = resolveChatWorkExecutionFootersWithContinuity(
      cadenceStore,
      views.events,
      sampledAtMs,
      { activeChatKey: chatKey },
    );
    cadenceStore = liveStart.store;
    const liveStartFooter = liveStart.footersByEventKey.get(eventKey)!;
    assert.equal(liveStartFooter.statusKey, 'working');
    assert.equal(liveStartFooter.statusLabel, 'Started');
    assert.equal(liveStartFooter.tone, 'neutral');
    assert.equal(liveStartFooter.duration?.kind, 'elapsed');
    assert.equal(liveStartFooter.durationLabel, `Elapsed ${(sampledAtMs - 45_000) / 1_000}s`);
    assert.equal(
      liveStartFooter.statusTitle,
      'Started, live evidence fresh (last confirmed just now)',
    );
    assert.equal(liveStartFooter.statusAriaLabel, liveStartFooter.statusTitle);
    assert.ok(sampledAtMs - 45_000 > previousFrozenMs);

    const liveTickMs = sampledAtMs + 2_000;
    const expectedFrozenMs = liveTickMs - 45_000;
    const liveTick = resolveChatWorkExecutionFootersWithContinuity(
      cadenceStore,
      views.events,
      liveTickMs,
      { activeChatKey: chatKey },
    );
    cadenceStore = liveTick.store;
    assert.equal(
      liveTick.footersByEventKey.get(eventKey)!.durationLabel,
      `Elapsed ${expectedFrozenMs / 1_000}s`,
    );

    const gapTick = resolveChatWorkExecutionFootersWithContinuity(
      cadenceStore,
      views.events,
      expiresAtMs + 500,
      { activeChatKey: chatKey },
    );
    cadenceStore = gapTick.store;
    const gapFooter = gapTick.footersByEventKey.get(eventKey)!;
    assert.equal(gapFooter.statusKey, 'started');
    assert.equal(gapFooter.statusLabel, 'Started');
    assert.equal(gapFooter.tone, 'neutral');
    assert.deepEqual(gapFooter.duration, {
      kind: 'last_confirmed',
      elapsedMs: expectedFrozenMs,
    });
    assert.equal(gapFooter.durationLabel, `Elapsed ${expectedFrozenMs / 1_000}s`);
    assert.equal(
      gapFooter.statusTitle,
      'Started, last confirmed status (confirmed 3.5s ago)',
    );
    assert.equal(gapFooter.statusAriaLabel, gapFooter.statusTitle);
    assert.equal(
      gapFooter.durationTitle,
      `Last confirmed elapsed ${expectedFrozenMs / 1_000}s (confirmed 3.5s ago)`,
    );
    assert.equal(gapFooter.durationAriaLabel, gapFooter.durationTitle);

    // Later inside the same gap elapsed stays frozen while confirmation age advances.
    const gapLater = resolveChatWorkExecutionFootersWithContinuity(
      cadenceStore,
      views.events,
      expiresAtMs + 900,
      { activeChatKey: chatKey },
    );
    cadenceStore = gapLater.store;
    const gapLaterFooter = gapLater.footersByEventKey.get(eventKey)!;
    assert.equal(gapLaterFooter.durationLabel, `Elapsed ${expectedFrozenMs / 1_000}s`);
    assert.equal(
      gapLaterFooter.statusTitle,
      'Started, last confirmed status (confirmed 3.9s ago)',
    );
    previousFrozenMs = expectedFrozenMs;
  }

  const recoveredViews = buildViews(
    [
      makePresence({
        item_id: 'blg_gap',
        execution_ref: 'exec-gap-1',
        sequence: 23,
        sampled_at_ms: 62_000,
        sample_age_ms: 0,
        max_sample_age_ms: 3_000,
        expires_at_ms: 65_000,
      }),
    ],
    { started_at_ms: 45_000 },
  );
  const recovered = resolveChatWorkExecutionFootersWithContinuity(
    cadenceStore,
    recoveredViews.events,
    62_000,
    { activeChatKey: chatKey },
  );
  const recoveredFooter = recovered.footersByEventKey.get(eventKey)!;
  assert.equal(recoveredFooter.statusKey, 'working');
  assert.equal(recoveredFooter.statusLabel, 'Started');
  assert.equal(recoveredFooter.duration?.kind, 'elapsed');
  assert.equal(recoveredFooter.durationLabel, 'Elapsed 17s');
});

test('worker continuity immediately clears on explicit non_live, restarted host, foreign runs, other chats, and terminal failure/success', () => {
  const alphaIdentity = makeIdentity('bnd_alpha_cont_iso', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_beta_cont_iso', UUID_BETA);
  const alphaChatKey = resolveMessageScrollChatKey(alphaIdentity);
  const betaChatKey = resolveMessageScrollChatKey(betaIdentity);

  const buildChatViews = (
    identity: WorkspaceChatIdentity,
    presenceRows: readonly ChatWorkPresence[],
    executionOverrides: ChatWorkExecutionFixtureOverrides = {},
    itemOverrides: Partial<ChatWorkItem> = {},
  ) => {
    const item = makeBacklogItem({
      item_id: 'blg_iso',
      binding_id: identity.binding_id,
      kind: 'worker',
      status: 'doing',
      current_execution_ref: 'exec-iso-1',
      ...itemOverrides,
    });
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [item],
          events: [
            makeBacklogEvent({
              event_id: 'blg_iso:3',
              sequence: 3,
              item_id: 'blg_iso',
              revision: 3,
              action: 'worker_execution_attached',
              status: 'doing',
              execution_ref: 'exec-iso-1',
              recorded_at_ms: 9_100,
            }),
          ],
          worker_presence: [...presenceRows],
          worker_executions: [
            makeExecution(identity.binding_id, {
              item_id: 'blg_iso',
              execution_ref: 'exec-iso-1',
              worker_status: 'running',
              outcome: null,
              started_at_ms: 9_000,
              ...executionOverrides,
            }),
          ],
        }),
      ],
      identity,
    );
    return buildChatWorkEventViews(selection);
  };

  const livePresence = makePresence({
    item_id: 'blg_iso',
    execution_ref: 'exec-iso-1',
    host_instance_id: 'host-1',
    invocation_id: 'invocation-1',
    claim_id: 'claim-1',
    sequence: 8,
    sampled_at_ms: 10_000,
    max_sample_age_ms: 5_000,
    expires_at_ms: 15_000,
  });

  const seedStore = () => {
    const events = buildChatViews(alphaIdentity, [livePresence]);
    const seeded = resolveChatWorkExecutionFootersWithContinuity(
      createChatWorkExecutionContinuityStore(),
      events,
      14_000,
      { activeChatKey: alphaChatKey },
    );
    assert.equal(seeded.footersByEventKey.get(events[0]!.key)!.statusKey, 'working');
    return seeded.store;
  };

  // 1. Explicit non_live immediately clears Started -> Status unavailable.
  {
    const events = buildChatViews(alphaIdentity, [
      makePresence({
        ...livePresence,
        sequence: 9,
        availability: 'non_live',
        freshness: 'not_applicable',
        live: false,
        sampled_at_ms: 15_200,
        expires_at_ms: null,
      }),
    ]);
    const next = resolveChatWorkExecutionFootersWithContinuity(
      seedStore(),
      events,
      15_500,
      { activeChatKey: alphaChatKey },
    );
    const footer = next.footersByEventKey.get(events[0]!.key)!;
    assert.equal(footer.statusKey, 'unavailable');
    assert.equal(footer.duration, null);
  }

  // 2. Restarted host, changed invocation/claim, missing host, or future sample
  // immediately invalidates continuity.
  for (const invalidPresence of [
    makePresence({
      ...livePresence,
      host_instance_id: 'host-restarted-2',
      availability: 'unavailable',
      freshness: 'not_applicable',
      live: false,
      sampled_at_ms: 15_200,
      expires_at_ms: null,
    }),
    makePresence({
      ...livePresence,
      invocation_id: 'invocation-restarted-2',
      availability: 'unavailable',
      freshness: 'not_applicable',
      live: false,
      sampled_at_ms: 15_200,
      expires_at_ms: null,
    }),
    makePresence({
      ...livePresence,
      claim_id: 'claim-2',
      availability: 'unavailable',
      freshness: 'not_applicable',
      live: false,
      sampled_at_ms: 15_200,
      expires_at_ms: null,
    }),
    makePresence({
      ...livePresence,
      host_instance_id: null,
      availability: 'unavailable',
      freshness: 'not_applicable',
      live: false,
      sampled_at_ms: 15_200,
      expires_at_ms: null,
    }),
    makePresence({
      ...livePresence,
      sampled_at_ms: 16_000,
      expires_at_ms: 21_000,
    }),
  ]) {
    const events = buildChatViews(alphaIdentity, [invalidPresence]);
    const next = resolveChatWorkExecutionFootersWithContinuity(
      seedStore(),
      events,
      15_500,
      { activeChatKey: alphaChatKey },
    );
    const footer = next.footersByEventKey.get(events[0]!.key)!;
    assert.equal(footer.statusKey, 'unavailable');
    assert.equal(footer.duration, null);
  }

  // 3. Foreign run (changed run_id/worker_id) never inherits Started or elapsed.
  {
    const foreignEvents = buildChatViews(
      alphaIdentity,
      [
        makePresence({
          ...livePresence,
          run_id: 'run-foreign-2',
          worker_id: 'worker-foreign-2',
          freshness: 'stale',
          live: false,
        }),
      ],
      { run_id: 'run-foreign-2', worker_id: 'worker-foreign-2' },
    );
    const next = resolveChatWorkExecutionFootersWithContinuity(
      seedStore(),
      foreignEvents,
      15_500,
      { activeChatKey: alphaChatKey },
    );
    const footer = next.footersByEventKey.get(foreignEvents[0]!.key)!;
    assert.equal(footer.statusKey, 'unavailable');
    assert.equal(footer.duration, null);
  }

  // 4. Sibling chat with identical item_id/execution_ref/run_id never inherits
  // continuity from Alpha.
  {
    const betaEvents = buildChatViews(betaIdentity, [
      makePresence({ ...livePresence, freshness: 'stale', live: false }),
    ]);
    const next = resolveChatWorkExecutionFootersWithContinuity(
      seedStore(),
      betaEvents,
      15_500,
      { activeChatKey: betaChatKey },
    );
    const footer = next.footersByEventKey.get(betaEvents[0]!.key)!;
    assert.equal(footer.statusKey, 'unavailable');
    assert.equal(footer.duration, null);
  }

  // 5. Terminal failure and terminal success (Awaiting check / Completed)
  // immediately win during a brief gap and evict continuity.
  for (const terminalCase of [
    {
      execution: {
        worker_status: 'failed',
        outcome: 'failure' as const,
        finished_at_ms: 15_400,
        duration_ms: 6_400,
      },
      item: { status: 'blocked' as const, blocked_reason: 'Test failed.' },
      expectedStatus: 'failed' as const,
      expectedLabel: 'Failed',
      expectedDuration: '6.4s',
    },
    {
      execution: {
        worker_status: 'completed',
        outcome: 'success' as const,
        acceptance: 'pending_review',
        finished_at_ms: 15_400,
        duration_ms: 6_400,
      },
      item: { status: 'doing' as const },
      expectedStatus: 'checking' as const,
      expectedLabel: 'Awaiting check',
      expectedDuration: '6.4s',
    },
    {
      execution: {
        worker_status: 'completed',
        outcome: 'success' as const,
        acceptance: 'accepted',
        finished_at_ms: 15_400,
        duration_ms: 6_400,
      },
      item: { status: 'done' as const, completed_at_ms: 15_400 },
      expectedStatus: 'completed' as const,
      expectedLabel: 'Completed',
      expectedDuration: '6.4s',
    },
  ]) {
    const events = buildChatViews(
      alphaIdentity,
      [livePresence],
      terminalCase.execution,
      terminalCase.item,
    );
    const next = resolveChatWorkExecutionFootersWithContinuity(
      seedStore(),
      events,
      15_500,
      { activeChatKey: alphaChatKey },
    );
    const footer = next.footersByEventKey.get(events[0]!.key)!;
    assert.equal(footer.statusKey, terminalCase.expectedStatus);
    assert.equal(footer.statusLabel, terminalCase.expectedLabel);
    assert.equal(footer.duration?.kind, 'final');
    assert.equal(footer.durationLabel, terminalCase.expectedDuration);
    assert.equal(next.store.size, 0);
  }
});

test('previously confirmed running attempt returns unavailable with no timer after empty worker_presence passes its real deadline without affecting never-observed dispatches, grace bounds, terminal precedence, or attempt/host/chat isolation', () => {
  const alphaIdentity = makeIdentity('bnd_alpha_empty_expiry', UUID_ALPHA);
  const betaIdentity = makeIdentity('bnd_beta_empty_expiry', UUID_BETA);
  const alphaChatKey = resolveMessageScrollChatKey(alphaIdentity);
  const betaChatKey = resolveMessageScrollChatKey(betaIdentity);

  const buildChatViews = (
    identity: WorkspaceChatIdentity,
    presenceRows: readonly ChatWorkPresence[],
    executionOverrides: ChatWorkExecutionFixtureOverrides = {},
    itemOverrides: Partial<ChatWorkItem> = {},
  ) => {
    const item = makeBacklogItem({
      item_id: 'blg_empty_expiry',
      binding_id: identity.binding_id,
      kind: 'worker',
      status: 'doing',
      current_execution_ref: 'exec-empty-1',
      ...itemOverrides,
    });
    const selection = selectWorkspaceChatWork(
      [
        makeChatWork(identity, {
          items: [item],
          events: [
            makeBacklogEvent({
              event_id: 'blg_empty_expiry:3',
              sequence: 3,
              item_id: 'blg_empty_expiry',
              revision: 3,
              action: 'worker_execution_attached',
              status: 'doing',
              execution_ref: 'exec-empty-1',
              recorded_at_ms: 8_100,
            }),
          ],
          worker_presence: [...presenceRows],
          worker_executions: [
            makeExecution(identity.binding_id, {
              item_id: 'blg_empty_expiry',
              execution_ref: 'exec-empty-1',
              worker_status: 'running',
              outcome: null,
              started_at_ms: 8_000,
              ...executionOverrides,
            }),
          ],
        }),
      ],
      identity,
    );
    return buildChatWorkEventViews(selection);
  };

  // Never-observed dispatch with empty worker_presence stays Dispatched.
  const emptyEvents = buildChatViews(alphaIdentity, []);
  const eventKey = emptyEvents[0]!.key;
  const attemptKey = emptyEvents[0]!.workerExecution!.attemptKey!;
  const neverObserved = resolveChatWorkExecutionFootersWithContinuity(
    createChatWorkExecutionContinuityStore(),
    emptyEvents,
    12_000,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(neverObserved.footersByEventKey.get(eventKey)!.statusKey, 'dispatched');
  assert.equal(neverObserved.footersByEventKey.get(eventKey)!.statusLabel, 'Dispatched');
  assert.equal(neverObserved.footersByEventKey.get(eventKey)!.duration, null);
  assert.equal(neverObserved.footersByEventKey.get(eventKey)!.durationLabel, null);
  assert.equal(neverObserved.store.size, 0);

  // Confirm live running (sampled at 10_000, max_sample_age_ms 3_000, expires at 13_000 ->
  // real deadline is 13_000 + min(5_000, 3_000) = 16_000).
  const livePresence = makePresence({
    item_id: 'blg_empty_expiry',
    execution_ref: 'exec-empty-1',
    host_instance_id: 'host-1',
    invocation_id: 'invocation-1',
    claim_id: 'claim-1',
    sequence: 10,
    sampled_at_ms: 10_000,
    sample_age_ms: 0,
    max_sample_age_ms: 3_000,
    expires_at_ms: 13_000,
  });
  let step = resolveChatWorkExecutionFootersWithContinuity(
    neverObserved.store,
    buildChatViews(alphaIdentity, [livePresence]),
    12_000,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(step.footersByEventKey.get(eventKey)!.statusKey, 'working');
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 4s');
  const activeEntry = step.store.get(attemptKey)!;
  assert.ok(activeEntry);

  // worker_presence becomes empty before real deadline -> holds Started with frozen elapsed;
  // omitted and replayed stale samples never extend expiresAtMs or real deadline (16_000).
  step = resolveChatWorkExecutionFootersWithContinuity(
    step.store,
    emptyEvents,
    13_100,
    { activeChatKey: alphaChatKey },
  );
  const openGapStore = step.store;
  assert.equal(step.footersByEventKey.get(eventKey)!.statusKey, 'started');
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 4s');
  assert.equal(step.store.get(attemptKey)?.expiresAtMs, 13_000);

  const replayedStaleEvents = buildChatViews(alphaIdentity, [
    makePresence({
      ...livePresence,
      sequence: 10,
      freshness: 'stale',
      live: false,
      sampled_at_ms: 14_000,
      sample_age_ms: 4_000,
      expires_at_ms: 17_000,
    }),
  ]);
  step = resolveChatWorkExecutionFootersWithContinuity(
    step.store,
    replayedStaleEvents,
    14_500,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(step.footersByEventKey.get(eventKey)!.statusKey, 'started');
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 4s');
  assert.equal(step.store.get(attemptKey)?.expiresAtMs, 13_000);

  step = resolveChatWorkExecutionFootersWithContinuity(
    step.store,
    emptyEvents,
    15_999,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(step.footersByEventKey.get(eventKey)!.statusKey, 'started');
  assert.equal(step.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 4s');

  // Crossing the exact real deadline (16_000) with empty worker_presence returns
  // Status unavailable with no timer (not Dispatched) and stays unavailable across later refreshes.
  for (const afterDeadlineMs of [16_000, 17_000, 25_000]) {
    step = resolveChatWorkExecutionFootersWithContinuity(
      step.store,
      emptyEvents,
      afterDeadlineMs,
      { activeChatKey: alphaChatKey },
    );
    const footer = step.footersByEventKey.get(eventKey)!;
    assert.equal(footer.statusKey, 'unavailable');
    assert.equal(footer.statusLabel, 'Status unavailable');
    assert.equal(footer.duration, null);
    assert.equal(footer.durationLabel, null);
    assert.equal(footer.statusAriaLabel, undefined);
    assert.equal(footer.statusTitle, undefined);
    assert.equal(step.store.get(attemptKey)?.briefGapClosed, true);
  }

  // Once closed, clock jitter or replayed stale data never re-enters Started.
  const closedStore = step.store;
  const jitterBack = resolveChatWorkExecutionFootersWithContinuity(
    closedStore,
    emptyEvents,
    15_500,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(jitterBack.footersByEventKey.get(eventKey)!.statusKey, 'unavailable');
  assert.equal(jitterBack.footersByEventKey.get(eventKey)!.duration, null);

  // Direct resolveChatWorkExecutionFooterState honors scoped matching continuity after expiry
  // while keeping mismatched or absent continuity on empty presence as Dispatched.
  const directExpired = resolveChatWorkExecutionFooterState(
    emptyEvents[0]!.workerExecution!,
    16_000,
    activeEntry,
  );
  assert.equal(directExpired.statusKey, 'unavailable');
  assert.equal(directExpired.duration, null);
  assert.equal(
    resolveChatWorkExecutionFooterState(emptyEvents[0]!.workerExecution!, 16_000, null)
      .statusKey,
    'dispatched',
  );

  // Isolation: another attempt on the same item/ref and a sibling chat with empty
  // worker_presence never inherit Started or Status unavailable.
  const foreignAttemptEmpty = buildChatViews(alphaIdentity, [], {
    run_id: 'run-foreign-empty',
    worker_id: 'worker-foreign-empty',
  });
  assert.equal(
    resolveChatWorkExecutionFooterState(
      foreignAttemptEmpty[0]!.workerExecution!,
      16_000,
      activeEntry,
    ).statusKey,
    'dispatched',
  );
  const foreignStep = resolveChatWorkExecutionFootersWithContinuity(
    closedStore,
    foreignAttemptEmpty,
    16_500,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(
    foreignStep.footersByEventKey.get(foreignAttemptEmpty[0]!.key)!.statusKey,
    'dispatched',
  );

  const betaEmpty = buildChatViews(betaIdentity, []);
  const betaAcrossAlphaExpiry = resolveChatWorkExecutionFootersWithContinuity(
    openGapStore,
    betaEmpty,
    16_500,
    { activeChatKey: betaChatKey },
  );
  assert.equal(
    betaAcrossAlphaExpiry.footersByEventKey.get(betaEmpty[0]!.key)!.statusKey,
    'dispatched',
  );
  assert.equal(betaAcrossAlphaExpiry.store.get(attemptKey)?.briefGapClosed, true);
  // Switching back to Alpha after crossing expiry while viewing Beta keeps Alpha unavailable.
  const alphaReturned = resolveChatWorkExecutionFootersWithContinuity(
    betaAcrossAlphaExpiry.store,
    emptyEvents,
    17_000,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(
    alphaReturned.footersByEventKey.get(eventKey)!.statusKey,
    'unavailable',
  );
  assert.equal(alphaReturned.footersByEventKey.get(eventKey)!.duration, null);

  // Host restart or explicit non_live inside the grace window closes brief-gap continuity so
  // subsequent empty worker_presence still within the old grace window stays unavailable.
  for (const invalidatingPresence of [
    makePresence({
      ...livePresence,
      host_instance_id: 'host-restarted-2',
      availability: 'unavailable',
      freshness: 'not_applicable',
      live: false,
      sampled_at_ms: 13_400,
      expires_at_ms: null,
    }),
    makePresence({
      ...livePresence,
      sequence: 11,
      availability: 'non_live',
      freshness: 'not_applicable',
      live: false,
      sampled_at_ms: 13_400,
      expires_at_ms: null,
    }),
  ]) {
    const seeded = resolveChatWorkExecutionFootersWithContinuity(
      createChatWorkExecutionContinuityStore(),
      buildChatViews(alphaIdentity, [livePresence]),
      12_000,
      { activeChatKey: alphaChatKey },
    );
    const invalidated = resolveChatWorkExecutionFootersWithContinuity(
      seeded.store,
      buildChatViews(alphaIdentity, [invalidatingPresence]),
      13_500,
      { activeChatKey: alphaChatKey },
    );
    assert.equal(invalidated.footersByEventKey.get(eventKey)!.statusKey, 'unavailable');
    const emptyStillInOldWindow = resolveChatWorkExecutionFootersWithContinuity(
      invalidated.store,
      emptyEvents,
      14_000,
      { activeChatKey: alphaChatKey },
    );
    assert.equal(
      emptyStillInOldWindow.footersByEventKey.get(eventKey)!.statusKey,
      'unavailable',
    );
    assert.equal(emptyStillInOldWindow.footersByEventKey.get(eventKey)!.duration, null);
  }

  // Terminal precedence immediately wins over the closed empty-presence marker and evicts it.
  const terminalEmpty = buildChatViews(
    alphaIdentity,
    [],
    {
      worker_status: 'completed',
      outcome: 'success',
      acceptance: 'accepted',
      finished_at_ms: 16_200,
      duration_ms: 8_200,
    },
    { status: 'done', completed_at_ms: 16_200 },
  );
  const terminalAfterExpiry = resolveChatWorkExecutionFootersWithContinuity(
    closedStore,
    terminalEmpty,
    17_000,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(
    terminalAfterExpiry.footersByEventKey.get(terminalEmpty[0]!.key)!.statusKey,
    'completed',
  );
  assert.equal(
    terminalAfterExpiry.footersByEventKey.get(terminalEmpty[0]!.key)!.durationLabel,
    '8.2s',
  );
  assert.equal(terminalAfterExpiry.store.size, 0);

  // Fresh live confirmation later recovers the same attempt from unavailable back to working.
  const recoveredLive = resolveChatWorkExecutionFootersWithContinuity(
    closedStore,
    buildChatViews(alphaIdentity, [
      makePresence({
        ...livePresence,
        sequence: 12,
        sampled_at_ms: 18_000,
        sample_age_ms: 0,
        max_sample_age_ms: 3_000,
        expires_at_ms: 21_000,
      }),
    ]),
    19_000,
    { activeChatKey: alphaChatKey },
  );
  assert.equal(recoveredLive.footersByEventKey.get(eventKey)!.statusKey, 'working');
  assert.equal(recoveredLive.footersByEventKey.get(eventKey)!.durationLabel, 'Elapsed 11s');
  assert.equal(recoveredLive.store.get(attemptKey)?.briefGapClosed, undefined);
});
