import type {
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
} from './observation/types.ts';
import type { ActivityFeedScope } from './pages/activity-feed-model.ts';
import {
  formatWorkspaceChatIdentityKey,
  isValidWorkspaceChatIdentity,
  workspaceChatIdentitiesEqual,
} from './workspace-contract.ts';
import type {
  WorkspaceChatIdentity,
  WorkspaceChatSummary,
  WorkspaceJobHistory,
  WorkspaceMessageItem,
} from './workspace-contract.ts';

/**
 * Pixel distance from the scrollport bottom within which the viewport is
 * considered pinned to the end (`follow` mode).
 */
export const DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX = 32 as const;

/**
 * Upper bound on remembered historical message IDs per chat so long-lived
 * sessions remain bounded even as the 1,000-item window trims older entries.
 */
export const MAX_TRACKED_SEEN_MESSAGE_IDS = 4_096 as const;

const UTF8_ENCODER = new TextEncoder();
const VISIBILITY_EPSILON_PX = 0.5;

function compareStringsUtf8(left: string, right: string): number {
  if (left === right) {
    return 0;
  }
  const leftBytes = UTF8_ENCODER.encode(left);
  const rightBytes = UTF8_ENCODER.encode(right);
  const minLen = Math.min(leftBytes.byteLength, rightBytes.byteLength);
  for (let idx = 0; idx < minLen; idx += 1) {
    const diff = (leftBytes[idx] ?? 0) - (rightBytes[idx] ?? 0);
    if (diff !== 0) {
      return diff;
    }
  }
  return leftBytes.byteLength - rightBytes.byteLength;
}

/**
 * Deterministic contract comparator matching `workspace-contract.ts`:
 * 1. `recorded_at_ms === null` sorts first (`0`), timestamped entries after (`1`)
 * 2. `recorded_at_ms` ascending
 * 3. `source_seq` ascending
 * 4. `sub_index` ascending
 * 5. UTF-8 byte order on `scope.source_id`, `source_kind`, `source_record_id`, `item_id`
 *
 * Ascending order places older items at the head and appends newer items at the bottom.
 */
export function compareWorkspaceMessageItems(
  left: WorkspaceMessageItem,
  right: WorkspaceMessageItem,
): number {
  const leftHasTs = left.source_order.recorded_at_ms !== null ? 1 : 0;
  const rightHasTs = right.source_order.recorded_at_ms !== null ? 1 : 0;
  if (leftHasTs !== rightHasTs) {
    return leftHasTs - rightHasTs;
  }
  const leftTs = left.source_order.recorded_at_ms ?? 0;
  const rightTs = right.source_order.recorded_at_ms ?? 0;
  if (leftTs !== rightTs) {
    return leftTs - rightTs;
  }
  if (left.source_order.source_seq !== right.source_order.source_seq) {
    return left.source_order.source_seq - right.source_order.source_seq;
  }
  if (left.source_order.sub_index !== right.source_order.sub_index) {
    return left.source_order.sub_index - right.source_order.sub_index;
  }
  const sourceCmp = compareStringsUtf8(left.scope.source_id, right.scope.source_id);
  if (sourceCmp !== 0) {
    return sourceCmp;
  }
  const kindCmp = compareStringsUtf8(left.source_kind, right.source_kind);
  if (kindCmp !== 0) {
    return kindCmp;
  }
  const recordCmp = compareStringsUtf8(left.source_record_id, right.source_record_id);
  if (recordCmp !== 0) {
    return recordCmp;
  }
  return compareStringsUtf8(left.item_id, right.item_id);
}

/**
 * Deduplicates items by stable `item_id` (retaining the latest payload when the
 * same `item_id` appears multiple times) and orders them deterministically so
 * new records append at the bottom.
 */
export function orderWorkspaceMessageItems(
  items: readonly WorkspaceMessageItem[],
): WorkspaceMessageItem[] {
  const byId = new Map<string, WorkspaceMessageItem>();
  for (const item of items) {
    if (typeof item.item_id !== 'string' || item.item_id.trim().length === 0) {
      continue;
    }
    byId.set(item.item_id, item);
  }
  return Array.from(byId.values()).sort(compareWorkspaceMessageItems);
}

function isChatSummary(
  value: WorkspaceChatSummary | WorkspaceChatIdentity,
): value is WorkspaceChatSummary {
  return 'identity' in value && typeof value.identity === 'object' && value.identity !== null;
}

/**
 * Resolves a canonical per-chat scroll state key. Never uses mutable display
 * labels (`thread_label`), `job_id`, or `sessionId` as chat identity.
 */
export function resolveMessageScrollChatKey(
  target: WorkspaceChatSummary | WorkspaceChatIdentity | string | null | undefined,
): string {
  if (target === null || target === undefined) {
    return '__none__';
  }
  if (typeof target === 'string') {
    const trimmed = target.trim();
    return trimmed.length > 0 ? `key:${trimmed}` : '__unscoped__';
  }
  const identity = isChatSummary(target) ? target.identity : target;
  const canonical = formatWorkspaceChatIdentityKey(identity);
  if (canonical !== null) {
    return canonical;
  }
  return [
    'wchat:fallback',
    identity.source_id,
    identity.binding_id,
    identity.thread_uuid,
    identity.canonical_project_scope,
  ].join('\u0000');
}

export interface MessageViewportMetrics {
  readonly scrollTop: number;
  readonly clientHeight: number;
  readonly scrollHeight: number;
}

export interface ContentOffsetMessageItem {
  readonly itemId: string;
  /** Item top offset relative to scrollable content origin (`0` at content top). */
  readonly offsetTopPx: number;
  /** Item border-box height in pixels. */
  readonly heightPx: number;
}

export interface MeasuredMessageItem {
  readonly itemId: string;
  /**
   * Pixel offset of the item's top edge relative to the scrollport's inner top edge.
   * Negative when the item starts above the visible viewport; `0` when aligned with
   * the top edge; positive when starting below top padding or a preceding gap.
   */
  readonly relativeTopPx: number;
  /** Rendered border-box height in pixels. */
  readonly heightPx: number;
}

export interface MessageScrollAnchor {
  readonly itemId: string;
  readonly offsetPx: number;
}

export type MessageScrollMode = 'follow' | 'reading';

export interface MessageScrollState {
  readonly chatKey: string;
  readonly initialized: boolean;
  readonly mode: MessageScrollMode;
  readonly anchor: MessageScrollAnchor | null;
  readonly orderedItemIds: readonly string[];
  readonly seenItemIds: ReadonlySet<string>;
  readonly unseenItemIds: ReadonlySet<string>;
  readonly pendingNewCount: number;
}

export type MessageScrollAction =
  | { readonly kind: 'none' }
  | { readonly kind: 'scroll_to_bottom' }
  | {
      readonly kind: 'preserve_anchor';
      readonly anchor: MessageScrollAnchor;
      readonly anchorWasTrimmed: boolean;
    };

export interface MessageScrollSyncResult {
  readonly state: MessageScrollState;
  readonly orderedItems: readonly WorkspaceMessageItem[];
  readonly action: MessageScrollAction;
}

export type MessageScrollResizeAction =
  | { readonly kind: 'none' }
  | { readonly kind: 'scroll_to_bottom'; readonly targetScrollTop: number }
  | {
      readonly kind: 'restore_anchor';
      readonly anchor: MessageScrollAnchor;
      readonly targetScrollTop: number;
    };

export interface MessageScrollResizeDecision {
  readonly state: MessageScrollState;
  readonly action: MessageScrollResizeAction;
}

function sanitizeMetric(value: number): number {
  return Number.isFinite(value) ? value : 0;
}

export function maxScrollTopPx(metrics: MessageViewportMetrics): number {
  const clientHeight = Math.max(0, sanitizeMetric(metrics.clientHeight));
  const scrollHeight = Math.max(0, sanitizeMetric(metrics.scrollHeight));
  return Math.max(0, scrollHeight - clientHeight);
}

export function distanceFromBottomPx(metrics: MessageViewportMetrics): number {
  const scrollTop = Math.max(0, sanitizeMetric(metrics.scrollTop));
  return Math.max(0, maxScrollTopPx(metrics) - scrollTop);
}

export function isNearBottom(
  metrics: MessageViewportMetrics,
  thresholdPx: number = DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
): boolean {
  const safeThreshold =
    Number.isFinite(thresholdPx) && thresholdPx >= 0
      ? thresholdPx
      : DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX;
  return distanceFromBottomPx(metrics) <= safeThreshold;
}

/**
 * Converts content-origin offsets (`offsetTop`, `height`) and `scrollTop` into
 * viewport-relative measurements matching `getBoundingClientRect()` geometry.
 */
export function measureItemsFromContentOffsets(
  items: readonly ContentOffsetMessageItem[],
  scrollTopPx: number,
): MeasuredMessageItem[] {
  const safeScrollTop = Math.max(0, sanitizeMetric(scrollTopPx));
  return items.map((item) => ({
    itemId: item.itemId,
    relativeTopPx: sanitizeMetric(item.offsetTopPx) - safeScrollTop,
    heightPx: Math.max(0, sanitizeMetric(item.heightPx)),
  }));
}

/**
 * Measures rendered message item elements inside a real DOM scroll container.
 * Uses `getBoundingClientRect()` relative to the scrollport's inner top border
 * edge (`scrollerRect.top + scroller.clientTop`) so sticky headers, padding,
 * gaps, or positioned wrappers never skew offsets.
 */
export function measureDomMessageViewport(
  scroller: HTMLElement,
  itemSelector: string = '[data-message-item-id]',
): { metrics: MessageViewportMetrics; items: MeasuredMessageItem[] } {
  const metrics: MessageViewportMetrics = {
    scrollTop: scroller.scrollTop,
    clientHeight: scroller.clientHeight,
    scrollHeight: scroller.scrollHeight,
  };
  const scrollerRect = scroller.getBoundingClientRect();
  const innerTopPx = scrollerRect.top + (scroller.clientTop || 0);
  const nodeList = scroller.querySelectorAll<HTMLElement>(itemSelector);
  const items: MeasuredMessageItem[] = [];
  for (const element of nodeList) {
    const itemId =
      element.getAttribute('data-message-item-id') ?? element.getAttribute('data-item-id');
    if (!itemId) {
      continue;
    }
    const rect = element.getBoundingClientRect();
    items.push({
      itemId,
      relativeTopPx: rect.top - innerTopPx,
      heightPx: rect.height,
    });
  }
  return { metrics, items };
}

/**
 * Captures the first message intersecting the visible viewport, along with its
 * exact pixel offset from the viewport top edge.
 */
export function captureFirstVisibleMessageAnchor(
  measuredItems: readonly MeasuredMessageItem[],
): MessageScrollAnchor | null {
  if (measuredItems.length === 0) {
    return null;
  }
  for (const item of measuredItems) {
    const isVisible =
      item.heightPx > 0
        ? item.relativeTopPx + item.heightPx > VISIBILITY_EPSILON_PX
        : item.relativeTopPx >= 0;
    if (isVisible) {
      return {
        itemId: item.itemId,
        offsetPx: item.relativeTopPx,
      };
    }
  }
  for (const item of measuredItems) {
    if (item.relativeTopPx + item.heightPx > 0) {
      return {
        itemId: item.itemId,
        offsetPx: item.relativeTopPx,
      };
    }
  }
  const last = measuredItems[measuredItems.length - 1]!;
  return {
    itemId: last.itemId,
    offsetPx: last.relativeTopPx,
  };
}

/**
 * Computes the clamped `scrollTop` that places the anchored item back at
 * `targetOffsetPx` from the scrollport top edge.
 */
export function computeAnchorScrollTop(
  metrics: MessageViewportMetrics,
  currentAnchorRelativeTopPx: number,
  targetOffsetPx: number,
): number {
  const currentScrollTop = Math.max(0, sanitizeMetric(metrics.scrollTop));
  const deltaPx = sanitizeMetric(currentAnchorRelativeTopPx) - sanitizeMetric(targetOffsetPx);
  const unclamped = currentScrollTop + deltaPx;
  const maxTop = maxScrollTopPx(metrics);
  return Math.min(maxTop, Math.max(0, unclamped));
}

function boundSeenSet(
  seen: Set<string>,
  protectedIds: readonly string[],
  maxSize: number = MAX_TRACKED_SEEN_MESSAGE_IDS,
): Set<string> {
  if (seen.size <= maxSize) {
    return seen;
  }
  const protectedSet = new Set(protectedIds);
  const excess = seen.size - maxSize;
  let removed = 0;
  const trimmed = new Set<string>();
  for (const id of seen) {
    if (removed < excess && !protectedSet.has(id)) {
      removed += 1;
      continue;
    }
    trimmed.add(id);
  }
  return trimmed;
}

export function createMessageScrollState(chatKey: string): MessageScrollState {
  return {
    chatKey,
    initialized: false,
    mode: 'follow',
    anchor: null,
    orderedItemIds: [],
    seenItemIds: new Set<string>(),
    unseenItemIds: new Set<string>(),
    pendingNewCount: 0,
  };
}

function resolveSurvivingAnchor(
  previousAnchor: MessageScrollAnchor | null,
  previousOrderedIds: readonly string[],
  nextOrderedIds: readonly string[],
  nextIdSet: ReadonlySet<string>,
): { anchor: MessageScrollAnchor | null; trimmed: boolean } {
  if (nextOrderedIds.length === 0) {
    return { anchor: null, trimmed: false };
  }
  if (previousAnchor !== null && nextIdSet.has(previousAnchor.itemId)) {
    return { anchor: previousAnchor, trimmed: false };
  }
  if (previousAnchor !== null) {
    const prevIdx = previousOrderedIds.indexOf(previousAnchor.itemId);
    if (prevIdx >= 0) {
      for (let idx = prevIdx + 1; idx < previousOrderedIds.length; idx += 1) {
        const candidateId = previousOrderedIds[idx]!;
        if (nextIdSet.has(candidateId)) {
          return {
            anchor: { itemId: candidateId, offsetPx: 0 },
            trimmed: true,
          };
        }
      }
      for (let idx = prevIdx - 1; idx >= 0; idx -= 1) {
        const candidateId = previousOrderedIds[idx]!;
        if (nextIdSet.has(candidateId)) {
          return {
            anchor: { itemId: candidateId, offsetPx: 0 },
            trimmed: true,
          };
        }
      }
    }
  }
  return {
    anchor: { itemId: nextOrderedIds[0]!, offsetPx: 0 },
    trimmed: previousAnchor !== null,
  };
}

/**
 * Reconciles incoming `WorkspaceMessageItem` records with the chat's current
 * scroll state:
 * - Orders items deterministically and deduplicates by `item_id`.
 * - In `follow` mode (including initial chat load), keeps viewport pinned to
 *   the bottom and marks all items as seen (`pendingNewCount === 0`).
 * - In `reading` mode, preserves the active anchor (falling back cleanly if
 *   head-trimming evicted the anchored item) and counts ONLY genuinely unseen
 *   `item_id`s toward `pendingNewCount`. Updating an already-known `item_id`
 *   never triggers `new-events`.
 */
export function syncMessageScrollItems(
  state: MessageScrollState,
  items: readonly WorkspaceMessageItem[],
): MessageScrollSyncResult {
  const orderedItems = orderWorkspaceMessageItems(items);
  const nextOrderedIds = orderedItems.map((item) => item.item_id);
  const nextIdSet = new Set(nextOrderedIds);

  if (!state.initialized || state.mode === 'follow') {
    const nextSeen = new Set(state.seenItemIds);
    for (const id of state.unseenItemIds) {
      nextSeen.add(id);
    }
    for (const id of nextOrderedIds) {
      nextSeen.add(id);
    }
    const boundedSeen = boundSeenSet(nextSeen, nextOrderedIds);
    const nextState: MessageScrollState = {
      chatKey: state.chatKey,
      initialized: true,
      mode: 'follow',
      anchor: null,
      orderedItemIds: nextOrderedIds,
      seenItemIds: boundedSeen,
      unseenItemIds: new Set<string>(),
      pendingNewCount: 0,
    };
    return {
      state: nextState,
      orderedItems,
      action: orderedItems.length > 0 ? { kind: 'scroll_to_bottom' } : { kind: 'none' },
    };
  }

  // `reading` mode: user scrolled up to inspect older records.
  const nextUnseen = new Set<string>();
  for (const existingUnseen of state.unseenItemIds) {
    if (nextIdSet.has(existingUnseen)) {
      nextUnseen.add(existingUnseen);
    }
  }
  for (const id of nextOrderedIds) {
    if (!state.seenItemIds.has(id) && !nextUnseen.has(id)) {
      nextUnseen.add(id);
    }
  }

  const boundedSeen = boundSeenSet(new Set(state.seenItemIds), nextOrderedIds);
  const { anchor: resolvedAnchor, trimmed } = resolveSurvivingAnchor(
    state.anchor,
    state.orderedItemIds,
    nextOrderedIds,
    nextIdSet,
  );

  const nextState: MessageScrollState = {
    chatKey: state.chatKey,
    initialized: true,
    mode: 'reading',
    anchor: resolvedAnchor,
    orderedItemIds: nextOrderedIds,
    seenItemIds: boundedSeen,
    unseenItemIds: nextUnseen,
    pendingNewCount: nextUnseen.size,
  };

  return {
    state: nextState,
    orderedItems,
    action:
      resolvedAnchor !== null
        ? {
            kind: 'preserve_anchor',
            anchor: resolvedAnchor,
            anchorWasTrimmed: trimmed,
          }
        : { kind: 'none' },
  };
}

/**
 * Updates scroll mode and first-visible-item anchor from a user scroll event.
 * - Within `thresholdPx` of the bottom: engages `follow`, clears anchor, and
 *   acknowledges any pending `new-events` items as seen.
 * - Above `thresholdPx`: engages `reading` and records the first visible item's
 *   `item_id` and exact pixel offset from the viewport top.
 */
export function handleMessageViewportScroll(
  state: MessageScrollState,
  metrics: MessageViewportMetrics,
  measuredItems: readonly MeasuredMessageItem[],
  thresholdPx: number = DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
): MessageScrollState {
  if (isNearBottom(metrics, thresholdPx)) {
    const nextSeen = new Set(state.seenItemIds);
    for (const id of state.unseenItemIds) {
      nextSeen.add(id);
    }
    for (const id of state.orderedItemIds) {
      nextSeen.add(id);
    }
    for (const item of measuredItems) {
      nextSeen.add(item.itemId);
    }
    return {
      chatKey: state.chatKey,
      initialized: true,
      mode: 'follow',
      anchor: null,
      orderedItemIds: state.orderedItemIds,
      seenItemIds: boundSeenSet(nextSeen, state.orderedItemIds),
      unseenItemIds: new Set<string>(),
      pendingNewCount: 0,
    };
  }

  const capturedAnchor = captureFirstVisibleMessageAnchor(measuredItems) ?? state.anchor;
  const nextSeen = new Set(state.seenItemIds);
  if (!state.initialized) {
    for (const id of state.orderedItemIds) {
      nextSeen.add(id);
    }
    for (const item of measuredItems) {
      nextSeen.add(item.itemId);
    }
  }

  return {
    chatKey: state.chatKey,
    initialized: true,
    mode: 'reading',
    anchor: capturedAnchor,
    orderedItemIds: state.orderedItemIds,
    seenItemIds: boundSeenSet(nextSeen, state.orderedItemIds),
    unseenItemIds: state.unseenItemIds,
    pendingNewCount: state.unseenItemIds.size,
  };
}

/**
 * Computes the deterministic scroll adjustment when the scrollport or message
 * content resizes without user scrolling:
 * - In `follow` mode: stays pinned to the new `maxScrollTopPx`.
 * - In `reading` mode: preserves the anchored item's exact `offsetPx` relative
 *   to the viewport top edge without altering `anchor.offsetPx`.
 */
export function resolveMessageViewportResize(
  state: MessageScrollState,
  metrics: MessageViewportMetrics,
  measuredItems: readonly MeasuredMessageItem[],
): MessageScrollResizeDecision {
  if (state.mode === 'follow') {
    if (measuredItems.length === 0 && state.orderedItemIds.length === 0) {
      return { state, action: { kind: 'none' } };
    }
    return {
      state,
      action: {
        kind: 'scroll_to_bottom',
        targetScrollTop: maxScrollTopPx(metrics),
      },
    };
  }

  const activeAnchor = state.anchor ?? captureFirstVisibleMessageAnchor(measuredItems);
  if (activeAnchor === null) {
    return { state, action: { kind: 'none' } };
  }

  const nextState =
    state.anchor === null
      ? {
          ...state,
          anchor: activeAnchor,
        }
      : state;

  const measuredAnchor = measuredItems.find((item) => item.itemId === activeAnchor.itemId);
  if (!measuredAnchor) {
    return { state: nextState, action: { kind: 'none' } };
  }

  const targetScrollTop = computeAnchorScrollTop(
    metrics,
    measuredAnchor.relativeTopPx,
    activeAnchor.offsetPx,
  );
  return {
    state: nextState,
    action: {
      kind: 'restore_anchor',
      anchor: activeAnchor,
      targetScrollTop,
    },
  };
}

/**
 * Explicit user action triggered by the single controlled `new-events` button:
 * marks all current items as seen, clears `pendingNewCount`, re-engages
 * `follow` mode, and instructs the viewport to scroll to the end.
 */
export function jumpToLatestMessages(state: MessageScrollState): {
  readonly state: MessageScrollState;
  readonly action: { readonly kind: 'scroll_to_bottom' };
} {
  const nextSeen = new Set(state.seenItemIds);
  for (const id of state.unseenItemIds) {
    nextSeen.add(id);
  }
  for (const id of state.orderedItemIds) {
    nextSeen.add(id);
  }
  return {
    state: {
      chatKey: state.chatKey,
      initialized: true,
      mode: 'follow',
      anchor: null,
      orderedItemIds: state.orderedItemIds,
      seenItemIds: boundSeenSet(nextSeen, state.orderedItemIds),
      unseenItemIds: new Set<string>(),
      pendingNewCount: 0,
    },
    action: { kind: 'scroll_to_bottom' },
  };
}

// ---------------------------------------------------------------------------
// Isolated Per-Chat Scroll State Store
// ---------------------------------------------------------------------------

export interface MessageScrollStore {
  readonly activeChatKey: string;
  readonly byChatKey: ReadonlyMap<string, MessageScrollState>;
}

export function createMessageScrollStore(
  initialChat?: WorkspaceChatSummary | WorkspaceChatIdentity | string | null,
): MessageScrollStore {
  const activeChatKey = resolveMessageScrollChatKey(initialChat);
  const byChatKey = new Map<string, MessageScrollState>();
  byChatKey.set(activeChatKey, createMessageScrollState(activeChatKey));
  return {
    activeChatKey,
    byChatKey,
  };
}

export function getActiveChatScrollState(store: MessageScrollStore): MessageScrollState {
  return (
    store.byChatKey.get(store.activeChatKey) ?? createMessageScrollState(store.activeChatKey)
  );
}

export function getChatScrollState(
  store: MessageScrollStore,
  target: WorkspaceChatSummary | WorkspaceChatIdentity | string | null | undefined,
): MessageScrollState {
  const key = resolveMessageScrollChatKey(target);
  return store.byChatKey.get(key) ?? createMessageScrollState(key);
}

/**
 * Switches the active chat without mixing scroll mode, anchors, or seen/unseen
 * `item_id` sets across chats. Returning to an earlier chat restores its own
 * isolated state.
 */
export function switchMessageScrollChat(
  store: MessageScrollStore,
  nextChat: WorkspaceChatSummary | WorkspaceChatIdentity | string | null | undefined,
): MessageScrollStore {
  const nextKey = resolveMessageScrollChatKey(nextChat);
  if (nextKey === store.activeChatKey && store.byChatKey.has(nextKey)) {
    return store;
  }
  const nextMap = new Map(store.byChatKey);
  if (!nextMap.has(nextKey)) {
    nextMap.set(nextKey, createMessageScrollState(nextKey));
  }
  return {
    activeChatKey: nextKey,
    byChatKey: nextMap,
  };
}

export function setChatScrollState(
  store: MessageScrollStore,
  chatState: MessageScrollState,
): MessageScrollStore {
  const nextMap = new Map(store.byChatKey);
  nextMap.set(chatState.chatKey, chatState);
  return {
    activeChatKey: store.activeChatKey,
    byChatKey: nextMap,
  };
}

// ---------------------------------------------------------------------------
// Strict Four-Part Chat Isolation & Job-Scope Intersection Helpers
// ---------------------------------------------------------------------------

function isUsableJobId(jobId: string | null | undefined): jobId is string {
  return (
    typeof jobId === 'string' &&
    jobId.length > 0 &&
    jobId.trim() === jobId &&
    jobId !== 'unknown'
  );
}

function resolveUnambiguousSourceProjectScope(
  sourceId: string,
  observation?: Pick<ObservationSnapshot, 'sources' | 'jobs'> | null,
  jobHistories?: readonly WorkspaceJobHistory[] | null,
): string | null {
  if (!observation) {
    return null;
  }
  const matchingSources = observation.sources.filter((src) => src.id === sourceId);
  if (matchingSources.length !== 1) {
    return null;
  }
  const rawPath = matchingSources[0]?.project_path ?? null;
  if (typeof rawPath !== 'string' || rawPath.length === 0 || rawPath.trim() !== rawPath) {
    return null;
  }

  for (const job of observation.jobs) {
    if (
      job.source_id === sourceId &&
      job.project_path !== null &&
      job.project_path !== rawPath
    ) {
      return null;
    }
  }

  if (jobHistories) {
    for (const history of jobHistories) {
      if (
        history.job.source_id === sourceId &&
        history.job.chat_identity !== null &&
        history.job.chat_identity.canonical_project_scope !== rawPath
      ) {
        return null;
      }
    }
  }

  return rawPath;
}

function doesJobHistoryConflictWithChat(
  history: WorkspaceJobHistory,
  chat: WorkspaceChatSummary,
): boolean {
  if (history.job.source_id !== chat.identity.source_id) {
    return true;
  }
  if (history.job.binding_id !== null && history.job.binding_id !== chat.identity.binding_id) {
    return true;
  }
  if (history.runs.some((run) => run.binding_id !== chat.identity.binding_id)) {
    return true;
  }
  if (
    history.job.chat_identity !== null &&
    (!isValidWorkspaceChatIdentity(history.job.chat_identity) ||
      !workspaceChatIdentitiesEqual(history.job.chat_identity, chat.identity))
  ) {
    return true;
  }
  return false;
}

function doesObservationJobConflictWithChat(
  job: ObservationJob,
  chat: WorkspaceChatSummary,
): boolean {
  if (job.source_id !== chat.identity.source_id) {
    return true;
  }
  if (job.binding_id !== null && job.binding_id !== chat.identity.binding_id) {
    return true;
  }
  if (job.thread_uuid !== null && job.thread_uuid !== chat.identity.thread_uuid) {
    return true;
  }
  if (job.project_path !== null && job.project_path !== chat.identity.canonical_project_scope) {
    return true;
  }
  return false;
}

/**
 * Collects verified per-chat `job_id`s on `chat.identity.source_id` backed by an
 * exact four-part `WorkspaceChatIdentity` and free of conflicting sibling records.
 */
export function resolveVerifiedChatJobIds(
  chat: WorkspaceChatSummary | null | undefined,
  jobHistories?: readonly WorkspaceJobHistory[] | null,
  observation?: Pick<ObservationSnapshot, 'sources' | 'jobs'> | null,
): ReadonlySet<string> {
  if (!chat || !isValidWorkspaceChatIdentity(chat.identity)) {
    return new Set<string>();
  }

  const verified = new Set<string>();
  const conflicted = new Set<string>();

  if (jobHistories) {
    for (const history of jobHistories) {
      if (history.job.source_id !== chat.identity.source_id) {
        continue;
      }
      const jobId = history.job.job_id;
      if (!isUsableJobId(jobId)) {
        continue;
      }
      if (doesJobHistoryConflictWithChat(history, chat)) {
        conflicted.add(jobId);
        continue;
      }
      if (history.job.chat_identity !== null) {
        verified.add(jobId);
      }
    }
  }

  if (observation) {
    const unambiguousSourcePath = resolveUnambiguousSourceProjectScope(
      chat.identity.source_id,
      observation,
      jobHistories,
    );
    for (const obsJob of observation.jobs) {
      if (obsJob.source_id !== chat.identity.source_id) {
        continue;
      }
      if (!isUsableJobId(obsJob.id)) {
        continue;
      }
      if (doesObservationJobConflictWithChat(obsJob, chat)) {
        conflicted.add(obsJob.id);
        continue;
      }
      const resolvedScopePath = obsJob.project_path ?? unambiguousSourcePath;
      if (
        obsJob.binding_id !== null &&
        obsJob.thread_uuid !== null &&
        resolvedScopePath !== null
      ) {
        const candidateIdentity = {
          source_id: obsJob.source_id,
          binding_id: obsJob.binding_id,
          thread_uuid: obsJob.thread_uuid,
          canonical_project_scope: resolvedScopePath,
        };
        if (
          isValidWorkspaceChatIdentity(candidateIdentity) &&
          workspaceChatIdentitiesEqual(candidateIdentity, chat.identity)
        ) {
          verified.add(obsJob.id);
        }
      }
    }
  }

  for (const id of conflicted) {
    verified.delete(id);
  }
  return verified;
}

/**
 * Determines whether a `WorkspaceJobHistory` belongs to a selected `chat`.
 * Requires an exact four-part `WorkspaceChatIdentity` (or a verified per-chat
 * known job mapping with all available fields agreeing). Never infers ownership
 * from `source_id`/`binding_id` alone or absence of sibling jobs.
 */
export function doesJobBelongToChat(
  history: WorkspaceJobHistory,
  chat: WorkspaceChatSummary,
  knownChatJobIds?: ReadonlySet<string>,
): boolean {
  if (!isValidWorkspaceChatIdentity(chat.identity)) {
    return false;
  }
  if (doesJobHistoryConflictWithChat(history, chat)) {
    return false;
  }
  if (history.job.chat_identity !== null) {
    return true;
  }
  if (
    knownChatJobIds &&
    isUsableJobId(history.job.job_id) &&
    knownChatJobIds.has(history.job.job_id)
  ) {
    return true;
  }
  return false;
}

export function filterJobsForChat(
  jobHistories: readonly WorkspaceJobHistory[],
  chat: WorkspaceChatSummary | null,
  knownChatJobIds?: ReadonlySet<string>,
): WorkspaceJobHistory[] {
  if (!chat) {
    return [...jobHistories];
  }
  if (!isValidWorkspaceChatIdentity(chat.identity)) {
    return [];
  }

  const conflictedIds = new Set<string>();
  for (const entry of jobHistories) {
    if (
      entry.job.source_id === chat.identity.source_id &&
      isUsableJobId(entry.job.job_id) &&
      doesJobHistoryConflictWithChat(entry, chat)
    ) {
      conflictedIds.add(entry.job.job_id);
    }
  }

  const safeKnownIds = knownChatJobIds
    ? new Set(Array.from(knownChatJobIds).filter((id) => !conflictedIds.has(id)))
    : undefined;

  return jobHistories.filter(
    (history) =>
      !conflictedIds.has(history.job.job_id) &&
      doesJobBelongToChat(history, chat, safeKnownIds),
  );
}

function doesMessageConflictWithChat(
  item: WorkspaceMessageItem,
  chat: WorkspaceChatSummary,
): boolean {
  const { scope } = item;
  if (scope.source_id !== chat.identity.source_id) {
    return true;
  }
  if (scope.binding_id !== null && scope.binding_id !== chat.identity.binding_id) {
    return true;
  }
  if (scope.thread_uuid !== null && scope.thread_uuid !== chat.identity.thread_uuid) {
    return true;
  }
  if (
    scope.canonical_project_scope !== null &&
    scope.canonical_project_scope !== chat.identity.canonical_project_scope
  ) {
    return true;
  }
  if (
    scope.chat_identity !== null &&
    (!isValidWorkspaceChatIdentity(scope.chat_identity) ||
      !workspaceChatIdentitiesEqual(scope.chat_identity, chat.identity))
  ) {
    return true;
  }
  return false;
}

/**
 * Determines whether a `WorkspaceMessageItem` belongs to a selected `chat`.
 * Requires an exact four-part `WorkspaceChatIdentity` or a genuine verified
 * per-chat known job mapping with all available scope fields agreeing.
 */
export function doesMessageBelongToChat(
  item: WorkspaceMessageItem,
  chat: WorkspaceChatSummary,
  knownChatJobIds?: ReadonlySet<string>,
): boolean {
  if (!isValidWorkspaceChatIdentity(chat.identity)) {
    return false;
  }
  if (doesMessageConflictWithChat(item, chat)) {
    return false;
  }
  const { scope } = item;
  if (scope.chat_identity !== null) {
    return true;
  }
  if (
    scope.binding_id !== null &&
    scope.thread_uuid !== null &&
    scope.canonical_project_scope !== null
  ) {
    const candidateIdentity = {
      source_id: scope.source_id,
      binding_id: scope.binding_id,
      thread_uuid: scope.thread_uuid,
      canonical_project_scope: scope.canonical_project_scope,
    };
    return (
      isValidWorkspaceChatIdentity(candidateIdentity) &&
      workspaceChatIdentitiesEqual(candidateIdentity, chat.identity)
    );
  }
  if (
    knownChatJobIds &&
    isUsableJobId(scope.job_id) &&
    knownChatJobIds.has(scope.job_id)
  ) {
    return true;
  }
  return false;
}

export function filterMessagesForChat(
  items: readonly WorkspaceMessageItem[],
  chat: WorkspaceChatSummary | null,
  knownChatJobIds?: ReadonlySet<string>,
): WorkspaceMessageItem[] {
  if (!chat) {
    return [...items];
  }
  if (!isValidWorkspaceChatIdentity(chat.identity)) {
    return [];
  }

  const conflictedJobIds = new Set<string>();
  for (const entry of items) {
    if (
      entry.scope.source_id === chat.identity.source_id &&
      isUsableJobId(entry.scope.job_id) &&
      doesMessageConflictWithChat(entry, chat)
    ) {
      conflictedJobIds.add(entry.scope.job_id);
    }
  }

  const safeKnownIds = knownChatJobIds
    ? new Set(Array.from(knownChatJobIds).filter((id) => !conflictedJobIds.has(id)))
    : undefined;

  return items.filter(
    (item) =>
      (!isUsableJobId(item.scope.job_id) || !conflictedJobIds.has(item.scope.job_id)) &&
      doesMessageBelongToChat(item, chat, safeKnownIds),
  );
}

export interface ObservationChatMatchContext {
  readonly knownChatJobIds?: ReadonlySet<string>;
  readonly observation?: Pick<ObservationSnapshot, 'sources' | 'jobs'> | null;
  readonly jobHistories?: readonly WorkspaceJobHistory[] | null;
}

/**
 * Determines whether an `ObservationRequest` belongs to a selected `chat`.
 * Never infers ownership from absence of sibling requests, partial binding/thread
 * fields, or display labels (`thread_label`, `project_label`).
 */
export function doesObservationRequestMatchChat(
  request: ObservationRequest,
  chat: WorkspaceChatSummary,
  context?: ObservationChatMatchContext,
): boolean {
  if (!isValidWorkspaceChatIdentity(chat.identity)) {
    return false;
  }
  if (request.source_id !== chat.identity.source_id) {
    return false;
  }
  if (request.binding_id !== null && request.binding_id !== chat.identity.binding_id) {
    return false;
  }
  if (request.thread_uuid !== null && request.thread_uuid !== chat.identity.thread_uuid) {
    return false;
  }

  const jobId = isUsableJobId(request.job_id) ? request.job_id : null;
  let linkedObsJob: ObservationJob | null = null;

  if (jobId !== null && context?.observation) {
    const matchingObsJobs = context.observation.jobs.filter(
      (candidate) => candidate.source_id === request.source_id && candidate.id === jobId,
    );
    for (const candidate of matchingObsJobs) {
      if (doesObservationJobConflictWithChat(candidate, chat)) {
        return false;
      }
    }
    if (matchingObsJobs.length === 1) {
      linkedObsJob = matchingObsJobs[0] ?? null;
    }
  }

  if (jobId !== null && context?.jobHistories) {
    for (const history of context.jobHistories) {
      if (
        history.job.source_id === request.source_id &&
        history.job.job_id === jobId &&
        doesJobHistoryConflictWithChat(history, chat)
      ) {
        return false;
      }
    }
  }

  if (jobId !== null) {
    const derivedVerified = resolveVerifiedChatJobIds(
      chat,
      context?.jobHistories,
      context?.observation,
    );
    if (derivedVerified.has(jobId) || context?.knownChatJobIds?.has(jobId)) {
      return true;
    }
  }

  const effectiveBindingId = request.binding_id ?? linkedObsJob?.binding_id ?? null;
  const effectiveThreadUuid = request.thread_uuid ?? linkedObsJob?.thread_uuid ?? null;
  const effectiveProjectScope =
    linkedObsJob?.project_path ??
    resolveUnambiguousSourceProjectScope(
      request.source_id,
      context?.observation,
      context?.jobHistories,
    );

  if (
    effectiveBindingId !== null &&
    effectiveThreadUuid !== null &&
    effectiveProjectScope !== null
  ) {
    const candidateIdentity = {
      source_id: request.source_id,
      binding_id: effectiveBindingId,
      thread_uuid: effectiveThreadUuid,
      canonical_project_scope: effectiveProjectScope,
    };
    return (
      isValidWorkspaceChatIdentity(candidateIdentity) &&
      workspaceChatIdentitiesEqual(candidateIdentity, chat.identity)
    );
  }

  return false;
}

/**
 * Intersects optional job scope (`ActivityFeedScope`) with selected-chat scope.
 * When `chat` is selected, a job filter never bypasses chat isolation; when
 * `chat` is `null`, global scope retains honest unscoped records.
 */
export function filterObservationRequestsForActivity(
  requests: readonly ObservationRequest[],
  options: {
    readonly chat?: WorkspaceChatSummary | null;
    readonly jobScope?: ActivityFeedScope | null;
    readonly knownChatJobIds?: ReadonlySet<string>;
    readonly observation?: Pick<ObservationSnapshot, 'sources' | 'jobs'> | null;
    readonly jobHistories?: readonly WorkspaceJobHistory[] | null;
  },
): ObservationRequest[] {
  const { chat = null, jobScope = null, knownChatJobIds, observation, jobHistories } = options;

  const jobFiltered = jobScope
    ? requests.filter(
        (req) => req.source_id === jobScope.source_id && req.job_id === jobScope.job_id,
      )
    : [...requests];

  if (!chat) {
    return jobFiltered;
  }

  const context: ObservationChatMatchContext = {
    knownChatJobIds,
    observation,
    jobHistories,
  };

  return jobFiltered.filter((req) => doesObservationRequestMatchChat(req, chat, context));
}
