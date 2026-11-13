import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
  buildChatWorkBacklogGroups,
  buildChatWorkEventViews,
  buildChatWorkLivenessIndex,
  captureFirstVisibleMessageAnchor,
  compareWorkspaceMessageItems,
  computeAnchorScrollTop,
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
  getActiveChatScrollState,
  getChatScrollState,
  handleMessageViewportScroll,
  isNearBottom,
  jumpToLatestMessages,
  measureItemsFromContentOffsets,
  orderWorkspaceMessageItems,
  readWorkspaceChatWork,
  resolveMessageScrollChatKey,
  resolveMessageViewportResize,
  resolveVerifiedChatJobIds,
  selectWorkspaceChatWork,
  setChatScrollState,
  summarizeChatWorkLiveness,
  switchMessageScrollChat,
  syncChatWorkEventScroll,
  syncMessageScrollItems,
} from './message-scroll.ts';
import type {
  ChatWorkEvent,
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

function makeNativeSample(
  identity: WorkspaceChatIdentity,
  activity: 'working' | 'waiting_for_approval' | 'waiting_for_input' | 'idle',
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
