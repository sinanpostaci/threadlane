import type {
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
} from './observation/types.ts';
import type { ActivityFeedScope } from './pages/activity-feed-model.ts';
import {
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  evaluateWorkspaceNativeSampleAt,
  formatWorkspaceChatIdentityKey,
  isValidWorkspaceChatIdentity,
  workspaceChatIdentitiesEqual,
} from './workspace-contract.ts';
import type {
  WorkspaceChatIdentity,
  WorkspaceChatSummary,
  WorkspaceChatWork,
  WorkspaceJobHistory,
  WorkspaceMessageItem,
  WorkspaceNativeSample,
} from './workspace-contract.ts';

/**
 * Pixel distance from the scrollport bottom within which the viewport is
 * considered pinned to the end (`follow` mode). Shared by the chat-work stream.
 */
export const DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX = 32 as const;

/**
 * Upper bound on remembered historical message IDs per chat so long-lived
 * sessions remain bounded even as the 1,000-item window trims older entries.
 */
export const MAX_TRACKED_SEEN_MESSAGE_IDS = 4_096 as const;

const UTF8_ENCODER = new TextEncoder();
const VISIBILITY_EPSILON_PX = 0.5;

// ---------------------------------------------------------------------------
// Minimal Chat Work: frozen `WorkspaceSnapshot.chat_work` consumption
// ---------------------------------------------------------------------------

/**
 * UI-side bounds mirroring the frozen `WorkspaceSnapshot.chat_work` contract.
 * The runtime projection owns authoritative truncation; these limits only keep
 * malformed or unnormalized payloads from growing the desktop view past the
 * agreed bounded windows.
 */
export const MAX_CHAT_WORK_ITEMS = 200 as const;
export const MAX_CHAT_WORK_EVENTS = 500 as const;
export const MAX_CHAT_WORK_PRESENCE = 32 as const;

export type ChatWorkItem = WorkspaceChatWork['items'][number];
export type ChatWorkEvent = WorkspaceChatWork['events'][number];
export type ChatWorkPresence = WorkspaceChatWork['worker_presence'][number];
export type ChatWorkStatus = ChatWorkItem['status'];
export type ChatWorkKind = ChatWorkItem['kind'];
export type ChatWorkEventOutcome = ChatWorkEvent['outcome'];
export type ChatWorkTone =
  | 'working'
  | 'attention'
  | 'danger'
  | 'success'
  | 'neutral';

export const BACKLOG_STATUS_ORDER = ['todo', 'doing', 'done', 'blocked'] as const;

export const BACKLOG_STATUS_LABELS: Readonly<Record<ChatWorkStatus, string>> = {
  todo: 'Todo',
  doing: 'Doing',
  done: 'Done',
  blocked: 'Blocked',
};

const BACKLOG_STATUS_TONES: Readonly<Record<ChatWorkStatus, ChatWorkTone>> = {
  todo: 'neutral',
  doing: 'working',
  done: 'success',
  blocked: 'danger',
};

const BACKLOG_KIND_LABELS: Readonly<Record<ChatWorkKind, string>> = {
  worker: 'Worker',
  coworker: 'Coworker',
};

const CHAT_WORK_ACTION_LABELS: Readonly<Record<string, string>> = {
  created: 'Added',
  edited: 'Updated',
  started: 'Started',
  blocked: 'Blocked',
  completed: 'Done',
  worker_reserved: 'Preparing',
  worker_execution_attached: 'Worker dispatched',
  worker_outcome_recorded: 'Result recorded',
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

type ChatWorkCandidate = Record<string, unknown> & {
  readonly identity: WorkspaceChatIdentity;
};

function isChatWorkCandidate(value: unknown): value is ChatWorkCandidate {
  return isRecord(value) && isValidWorkspaceChatIdentity(value.identity);
}

function chatWorkCandidates(value: unknown): ChatWorkCandidate[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const candidates: ChatWorkCandidate[] = [];
  const values: readonly unknown[] = value;
  for (const candidate of values) {
    if (isChatWorkCandidate(candidate)) {
      candidates.push(candidate);
    }
  }
  return candidates;
}

function isNativeSampleCandidate(value: unknown): value is WorkspaceNativeSample {
  // The canonical chat identity is the only trusted key here. Every remaining
  // field is re-validated and normalized by evaluateWorkspaceNativeSampleAt.
  return isRecord(value) && isValidWorkspaceChatIdentity(value.chat_identity);
}

function nativeSampleCandidates(value: unknown): WorkspaceNativeSample[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const samples: WorkspaceNativeSample[] = [];
  const values: readonly unknown[] = value;
  for (const candidate of values) {
    if (isNativeSampleCandidate(candidate)) {
      samples.push(candidate);
    }
  }
  return samples;
}

function cleanNonNegativeInt(value: unknown, fallback = 0): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return fallback;
  }
  const truncated = Math.trunc(value);
  return truncated >= 0 ? truncated : fallback;
}

function cleanOptionalTimestamp(value: unknown): number | null {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  const truncated = Math.trunc(value);
  return truncated >= 0 ? truncated : null;
}

function cleanOptionalString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? value : null;
}

function isBacklogStatus(value: unknown): value is ChatWorkStatus {
  return (
    value === 'todo' || value === 'doing' || value === 'done' || value === 'blocked'
  );
}

function isBacklogKind(value: unknown): value is ChatWorkKind {
  return value === 'worker' || value === 'coworker';
}

function isUsableChatWorkItem(item: unknown): item is ChatWorkItem {
  if (!isRecord(item)) {
    return false;
  }
  return (
    typeof item.item_id === 'string' &&
    item.item_id.trim().length > 0 &&
    typeof item.binding_id === 'string' &&
    item.binding_id.trim().length > 0 &&
    typeof item.title === 'string' &&
    typeof item.revision === 'number' &&
    Number.isFinite(item.revision) &&
    isBacklogStatus(item.status) &&
    isBacklogKind(item.kind)
  );
}

function isUsableChatWorkEvent(event: unknown): event is ChatWorkEvent {
  if (!isRecord(event)) {
    return false;
  }
  return (
    typeof event.event_id === 'string' &&
    event.event_id.trim().length > 0 &&
    typeof event.item_id === 'string' &&
    event.item_id.trim().length > 0 &&
    typeof event.sequence === 'number' &&
    Number.isFinite(event.sequence) &&
    Number.isInteger(event.sequence) &&
    event.sequence >= 0 &&
    typeof event.action === 'string' &&
    event.action.trim().length > 0 &&
    typeof event.revision === 'number' &&
    Number.isFinite(event.revision) &&
    typeof event.recorded_at_ms === 'number' &&
    Number.isFinite(event.recorded_at_ms) &&
    isBacklogStatus(event.status)
  );
}

function isUsableChatWorkPresence(presence: unknown): presence is ChatWorkPresence {
  if (!isRecord(presence)) {
    return false;
  }
  return (
    typeof presence.item_id === 'string' &&
    presence.item_id.trim().length > 0 &&
    typeof presence.execution_ref === 'string' &&
    presence.execution_ref.trim().length > 0
  );
}

function compareChatWorkEvents(left: ChatWorkEvent, right: ChatWorkEvent): number {
  if (left.sequence !== right.sequence) {
    return left.sequence - right.sequence;
  }
  const leftRecorded = cleanOptionalTimestamp(left.recorded_at_ms) ?? 0;
  const rightRecorded = cleanOptionalTimestamp(right.recorded_at_ms) ?? 0;
  if (leftRecorded !== rightRecorded) {
    return leftRecorded - rightRecorded;
  }
  if (left.event_id !== right.event_id) {
    return left.event_id < right.event_id ? -1 : 1;
  }
  return left.revision - right.revision;
}

function capChatWorkTail<T>(
  items: readonly T[],
  max: number,
): { items: T[]; omitted: number } {
  if (items.length <= max) {
    return { items: Array.from(items), omitted: 0 };
  }
  const omitted = items.length - max;
  return { items: Array.from(items.slice(omitted)), omitted };
}

function capChatWorkHead<T>(
  items: readonly T[],
  max: number,
): { items: T[]; omitted: number } {
  if (items.length <= max) {
    return { items: Array.from(items), omitted: 0 };
  }
  return { items: Array.from(items.slice(0, max)), omitted: items.length - max };
}

export interface ChatWorkBoundsView {
  itemsOmitted: number;
  eventsOmitted: number;
  presenceOmitted: number;
}

export interface WorkspaceChatWorkSelection {
  chatKey: string | null;
  identity: WorkspaceChatIdentity | null;
  availability: 'available' | 'unavailable';
  items: ChatWorkItem[];
  events: ChatWorkEvent[];
  presence: ChatWorkPresence[];
  bounds: ChatWorkBoundsView;
  notices: string[];
  unavailableReason: string | null;
}

function emptyChatWorkSelection(
  identity: WorkspaceChatIdentity | null,
  reason: string | null,
): WorkspaceChatWorkSelection {
  return {
    chatKey: identity !== null ? formatWorkspaceChatIdentityKey(identity) : null,
    identity,
    availability: 'unavailable',
    items: [],
    events: [],
    presence: [],
    bounds: { itemsOmitted: 0, eventsOmitted: 0, presenceOmitted: 0 },
    notices: reason !== null ? [reason] : [],
    unavailableReason: reason,
  };
}

/**
 * Reads the additive `WorkspaceSnapshot.chat_work` section without requiring the
 * shared DTO integration to be present in this frontend worktree. Missing data
 * is treated as unavailable, never as inferred legacy job/message state.
 */
export function readWorkspaceChatWork(snapshot: unknown): readonly ChatWorkCandidate[] {
  if (!isRecord(snapshot)) {
    return [];
  }
  return chatWorkCandidates(snapshot.chat_work);
}

function buildChatWorkSelectionFromWork(
  identity: WorkspaceChatIdentity,
  work: ChatWorkCandidate,
): WorkspaceChatWorkSelection {
  const chatKey = formatWorkspaceChatIdentityKey(identity);
  const rawBounds: Record<string, unknown> = isRecord(work.bounds) ? work.bounds : {};
  const availability: 'available' | 'unavailable' =
    work.availability === 'available' ? 'available' : 'unavailable';

  const rawItems: readonly unknown[] = Array.isArray(work.items) ? work.items : [];
  const usableItems = rawItems.filter(isUsableChatWorkItem);
  const itemCap = capChatWorkHead(usableItems, MAX_CHAT_WORK_ITEMS);

  const rawEvents: readonly unknown[] = Array.isArray(work.events) ? work.events : [];
  const usableEvents = rawEvents.filter(isUsableChatWorkEvent).sort(compareChatWorkEvents);
  const dedupedEvents: ChatWorkEvent[] = [];
  const seenEventIds = new Set<string>();
  for (const event of usableEvents) {
    if (seenEventIds.has(event.event_id)) {
      continue;
    }
    seenEventIds.add(event.event_id);
    dedupedEvents.push(event);
  }
  const eventCap = capChatWorkTail(dedupedEvents, MAX_CHAT_WORK_EVENTS);

  const rawPresence: readonly unknown[] = Array.isArray(work.worker_presence)
    ? work.worker_presence
    : [];
  const usablePresence = rawPresence
    .filter(isUsableChatWorkPresence)
    .sort((left, right) => {
      const leftPriority =
        (left.live === true ? 4 : 0) +
        (left.freshness === 'fresh' ? 2 : 0) +
        (left.availability === 'live' ? 1 : 0);
      const rightPriority =
        (right.live === true ? 4 : 0) +
        (right.freshness === 'fresh' ? 2 : 0) +
        (right.availability === 'live' ? 1 : 0);
      if (leftPriority !== rightPriority) {
        return rightPriority - leftPriority;
      }
      if (left.item_id !== right.item_id) {
        return left.item_id < right.item_id ? -1 : 1;
      }
      return left.execution_ref < right.execution_ref ? -1 : 1;
    });
  const presenceCap = capChatWorkHead(usablePresence, MAX_CHAT_WORK_PRESENCE);

  const itemsOmitted =
    cleanNonNegativeInt(rawBounds.items_omitted) +
    (rawItems.length - usableItems.length) +
    itemCap.omitted;
  const eventsOmitted =
    cleanNonNegativeInt(rawBounds.events_omitted) +
    (rawEvents.length - usableEvents.length) +
    eventCap.omitted;
  const presenceOmitted =
    cleanNonNegativeInt(rawBounds.presence_omitted) +
    (rawPresence.length - usablePresence.length) +
    presenceCap.omitted;

  const notices: string[] = [];
  if (availability === 'unavailable') {
    notices.push('Backlog data is currently unavailable for this chat.');
  }
  if (itemsOmitted > 0) {
    notices.push(
      `${itemsOmitted} older backlog ${itemsOmitted === 1 ? 'item is' : 'items are'} outside this bounded window.`,
    );
  }
  if (eventsOmitted > 0) {
    notices.push(
      `${eventsOmitted} older stream ${eventsOmitted === 1 ? 'event is' : 'events are'} outside this bounded window.`,
    );
  }
  if (presenceOmitted > 0) {
    notices.push(
      `${presenceOmitted} worker presence ${presenceOmitted === 1 ? 'record is' : 'records are'} outside this bounded window.`,
    );
  }

  return {
    chatKey,
    identity,
    availability,
    items: itemCap.items,
    events: eventCap.items,
    presence: presenceCap.items,
    bounds: { itemsOmitted, eventsOmitted, presenceOmitted },
    notices,
    unavailableReason:
      availability === 'unavailable'
        ? 'Backlog data is currently unavailable for this chat.'
        : null,
  };
}

/**
 * Selects the exact canonical chat identity. Titles, project labels, binding
 * suffixes, session ids, and job ids are never used as identity substitutes.
 */
export function selectWorkspaceChatWork(
  chatWork: unknown,
  identity: WorkspaceChatIdentity | null | undefined,
): WorkspaceChatWorkSelection {
  if (identity === null || identity === undefined || !isValidWorkspaceChatIdentity(identity)) {
    return emptyChatWorkSelection(null, 'Chat identity is unavailable.');
  }
  for (const candidate of chatWorkCandidates(chatWork)) {
    if (workspaceChatIdentitiesEqual(candidate.identity, identity)) {
      return buildChatWorkSelectionFromWork(candidate.identity, candidate);
    }
  }
  return emptyChatWorkSelection(identity, 'No backlog data is recorded for this chat yet.');
}

export interface ChatWorkEventView {
  key: string;
  eventId: string;
  sequence: number;
  itemId: string;
  revision: number;
  action: string;
  actionLabel: string;
  recordedAtMs: number;
  title: string;
  status: ChatWorkStatus;
  statusLabel: string;
  executionRef: string | null;
  outcome: ChatWorkEventOutcome;
  note: string | null;
  tone: ChatWorkTone;
}

function humanizeChatWorkAction(action: string): string {
  const words = action.replace(/[_-]+/g, ' ').trim();
  if (words.length === 0) {
    return 'Updated';
  }
  return `${words.charAt(0).toUpperCase()}${words.slice(1)}`;
}

function resolveChatWorkActionLabel(event: ChatWorkEvent): string {
  if (event.action === 'worker_outcome_recorded') {
    if (event.outcome === 'success') {
      return 'Awaiting check';
    }
    if (event.outcome === 'failure') {
      return 'Failed';
    }
    if (event.outcome === 'unknown') {
      return 'Outcome unknown';
    }
    return 'Result recorded';
  }
  return CHAT_WORK_ACTION_LABELS[event.action] ?? humanizeChatWorkAction(event.action);
}

function resolveChatWorkEventTone(event: ChatWorkEvent): ChatWorkTone {
  if (event.status === 'done' || event.action === 'completed') {
    return 'success';
  }
  if (event.status === 'blocked' || event.action === 'blocked') {
    return 'danger';
  }
  if (event.action === 'worker_outcome_recorded') {
    if (event.outcome === 'success') {
      return 'success';
    }
    if (event.outcome === 'failure') {
      return 'danger';
    }
    return 'neutral';
  }
  // Reservations, Doing, edits, and dispatch receipts never use Working or
  // attention tones; only actual live presence may do that elsewhere.
  return 'neutral';
}

/**
 * Builds the chronological stream oldest to newest. Only the actual
 * `worker_outcome_recorded` event may carry an outcome; a later durable result
 * is never projected backwards onto earlier reservation or completion events.
 */
export function buildChatWorkEventViews(
  selection: WorkspaceChatWorkSelection,
): ChatWorkEventView[] {
  const chatKey = selection.chatKey ?? 'unscoped';
  return selection.events.map((event) => {
    const title = typeof event.title === 'string' ? event.title : '';
    return {
      key: `${chatKey}\u0000${event.event_id}\u0000${event.sequence}`,
      eventId: event.event_id,
      sequence: event.sequence,
      itemId: event.item_id,
      revision: event.revision,
      action: event.action,
      actionLabel: resolveChatWorkActionLabel(event),
      recordedAtMs: cleanOptionalTimestamp(event.recorded_at_ms) ?? 0,
      title,
      status: event.status,
      statusLabel: BACKLOG_STATUS_LABELS[event.status],
      executionRef: cleanOptionalString(event.execution_ref),
      outcome: event.action === 'worker_outcome_recorded' ? event.outcome : null,
      note: cleanOptionalString(event.note),
      tone: resolveChatWorkEventTone(event),
    };
  });
}

export interface ChatWorkBacklogItemView {
  key: string;
  itemId: string;
  revision: number;
  title: string;
  kind: ChatWorkKind;
  kindLabel: string;
  status: ChatWorkStatus;
  statusLabel: string;
  statusTone: ChatWorkTone;
  blockedReason: string | null;
  acceptanceCondition: string | null;
  deliveryDescription: string | null;
  completionNote: string | null;
  completionEvidence: string | null;
  completedAtMs: number | null;
  updatedAtMs: number | null;
}

export interface ChatWorkBacklogGroup {
  status: ChatWorkStatus;
  label: string;
  items: ChatWorkBacklogItemView[];
}

function buildChatWorkBacklogItemView(
  item: ChatWorkItem,
  chatKey: string | null,
): ChatWorkBacklogItemView {
  return {
    key: `${chatKey ?? 'unscoped'}\u0000${item.item_id}\u0000${item.revision}`,
    itemId: item.item_id,
    revision: item.revision,
    title: typeof item.title === 'string' ? item.title : '',
    kind: item.kind,
    kindLabel: BACKLOG_KIND_LABELS[item.kind],
    status: item.status,
    statusLabel: BACKLOG_STATUS_LABELS[item.status],
    statusTone: BACKLOG_STATUS_TONES[item.status],
    blockedReason: cleanOptionalString(item.blocked_reason),
    acceptanceCondition: cleanOptionalString(item.acceptance_condition),
    deliveryDescription: cleanOptionalString(item.delivery_description),
    completionNote: cleanOptionalString(item.completion_note),
    completionEvidence: cleanOptionalString(item.completion_evidence),
    completedAtMs: cleanOptionalTimestamp(item.completed_at_ms),
    updatedAtMs: cleanOptionalTimestamp(item.updated_at_ms),
  };
}

export function buildChatWorkBacklogGroups(
  selection: WorkspaceChatWorkSelection,
): ChatWorkBacklogGroup[] {
  const items = selection.items.map((item) =>
    buildChatWorkBacklogItemView(item, selection.chatKey),
  );
  return BACKLOG_STATUS_ORDER.map((status) => ({
    status,
    label: BACKLOG_STATUS_LABELS[status],
    items: items.filter((item) => item.status === status),
  }));
}

export function backlogStatusTone(status: ChatWorkStatus): ChatWorkTone {
  return BACKLOG_STATUS_TONES[status];
}

export interface ChatWorkAgentView {
  key: string;
  role: 'coworker' | 'worker';
  roleLabel: 'Coworker' | 'Worker';
  detail: string;
  itemId: string | null;
  working: boolean;
}

export interface ChatWorkLiveness {
  chatKey: string | null;
  identity: WorkspaceChatIdentity | null;
  workAvailable: boolean;
  items: ChatWorkItem[];
  events: ChatWorkEvent[];
  presence: ChatWorkPresence[];
  bounds: ChatWorkBoundsView;
  notices: string[];
  coworker: {
    samplePresent: boolean;
    availability: string;
    activity: string;
    freshness: string;
    live: boolean;
    waitingForHuman: boolean;
  };
  agents: ChatWorkAgentView[];
  workingAgentCount: number;
  isWorking: boolean;
  waitingForHuman: boolean;
  statusLabel: string | null;
  statusTone: ChatWorkTone;
}

export interface ChatWorkLivenessOptions {
  nowMs: number;
  staleThresholdMs?: number;
  isStale?: boolean;
  refreshFailed?: boolean;
}

function isLiveChatWorkWorkerPresence(
  presence: ChatWorkPresence,
  item: ChatWorkItem,
  nowMs: number,
): boolean {
  if (item.status !== 'doing' || item.current_execution_ref === null) {
    return false;
  }
  if (presence.execution_ref !== item.current_execution_ref) {
    return false;
  }
  if (presence.live !== true || presence.availability !== 'live') {
    return false;
  }
  if (presence.freshness !== 'fresh') {
    return false;
  }
  if (!Number.isFinite(nowMs)) {
    return false;
  }
  const sampledAtMs = cleanOptionalTimestamp(presence.sampled_at_ms);
  const sampleAgeMs = cleanOptionalTimestamp(presence.sample_age_ms);
  const maxSampleAgeMs = cleanOptionalTimestamp(presence.max_sample_age_ms);
  const expiresAtMs = cleanOptionalTimestamp(presence.expires_at_ms);
  if (
    sampledAtMs === null ||
    sampledAtMs <= 0 ||
    sampleAgeMs === null ||
    maxSampleAgeMs === null ||
    maxSampleAgeMs <= 0 ||
    expiresAtMs === null ||
    expiresAtMs <= 0
  ) {
    return false;
  }

  // Age the original wall-clock sample. A static `sample_age_ms` carried by an
  // old snapshot (or a re-rendered/refreshed snapshot without a new sample) must
  // never keep a Worker looking fresh.
  const currentAgeMs = nowMs - sampledAtMs;
  if (currentAgeMs < 0 || currentAgeMs > maxSampleAgeMs) {
    return false;
  }
  if (sampleAgeMs > maxSampleAgeMs) {
    return false;
  }

  // The deadline is mandatory and must be a real future instant that agrees
  // with the sample and its freshness TTL. Missing, already-expired, or
  // overlong/inconsistent deadlines fail closed.
  if (expiresAtMs <= sampledAtMs || nowMs >= expiresAtMs) {
    return false;
  }
  const latestConsistentDeadlineMs = sampledAtMs + maxSampleAgeMs;
  if (
    !Number.isFinite(latestConsistentDeadlineMs) ||
    expiresAtMs > latestConsistentDeadlineMs
  ) {
    return false;
  }
  return true;
}

function collectLiveChatWorkWorkerAgents(
  selection: WorkspaceChatWorkSelection,
  nowMs: number,
): ChatWorkAgentView[] {
  if (selection.availability !== 'available' || selection.identity === null) {
    return [];
  }
  const expectedBindingId = selection.identity.binding_id;
  const itemsById = new Map<string, ChatWorkItem>();
  for (const item of selection.items) {
    itemsById.set(item.item_id, item);
  }
  const agents: ChatWorkAgentView[] = [];
  const seen = new Set<string>();
  for (const presence of selection.presence) {
    const item = itemsById.get(presence.item_id);
    if (
      !item ||
      item.binding_id !== expectedBindingId ||
      !isLiveChatWorkWorkerPresence(presence, item, nowMs)
    ) {
      continue;
    }
    const key = `${selection.chatKey ?? 'unscoped'}\u0000worker\u0000${item.item_id}\u0000${item.current_execution_ref ?? ''}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    agents.push({
      key,
      role: 'worker',
      roleLabel: 'Worker',
      detail: item.title,
      itemId: item.item_id,
      working: true,
    });
  }
  return agents;
}

/**
 * Derives liveness and `NeedsYou` only from actual fresh native samples and
 * exactly correlated host-adapter worker presence. Durable backlog state such as
 * Doing, Blocked, Done, a failure, or an awaiting-check outcome is never used as
 * a liveness or human-request substitute.
 */
export function evaluateChatWorkLiveness(
  selection: WorkspaceChatWorkSelection,
  nativeSample: WorkspaceNativeSample | null | undefined,
  options: ChatWorkLivenessOptions,
): ChatWorkLiveness {
  const nowMs = Number.isFinite(options.nowMs) ? Math.trunc(options.nowMs) : Date.now();
  const staleThresholdMs =
    typeof options.staleThresholdMs === 'number' && options.staleThresholdMs > 0
      ? options.staleThresholdMs
      : WORKSPACE_SAMPLE_STALE_THRESHOLD_MS;
  const suppressed = Boolean(options.isStale || options.refreshFailed);

  const evaluated =
    nativeSample !== null && nativeSample !== undefined
      ? evaluateWorkspaceNativeSampleAt(nativeSample, nowMs, staleThresholdMs)
      : null;
  const coworkerAvailability = evaluated?.effective_availability ?? 'unavailable';
  const coworkerActivity = evaluated?.effective_activity ?? 'unknown';
  const coworkerFreshness = evaluated?.freshness ?? 'unknown';
  const coworkerLive =
    !suppressed &&
    coworkerAvailability === 'available' &&
    coworkerActivity === 'working';
  const coworkerWaiting =
    !suppressed &&
    coworkerAvailability === 'available' &&
    coworkerFreshness === 'fresh' &&
    (coworkerActivity === 'waiting_for_approval' ||
      coworkerActivity === 'waiting_for_input');

  const agents: ChatWorkAgentView[] = [];
  if (coworkerLive) {
    agents.push({
      key: `${selection.chatKey ?? 'unscoped'}\u0000coworker`,
      role: 'coworker',
      roleLabel: 'Coworker',
      detail: 'Working in Codex',
      itemId: null,
      working: true,
    });
  }
  if (!suppressed) {
    agents.push(...collectLiveChatWorkWorkerAgents(selection, nowMs));
  }

  const workingAgentCount = agents.filter((agent) => agent.working).length;
  const isWorking = workingAgentCount > 0;
  const waitingForHuman = coworkerWaiting;
  const hasFreshNativeChat =
    !suppressed &&
    coworkerAvailability === 'available' &&
    coworkerFreshness === 'fresh';
  const statusLabel = waitingForHuman
    ? 'Waiting for you in Codex'
    : isWorking
      ? 'Working'
      : hasFreshNativeChat
        ? 'Codex chat'
        : null;
  const statusTone: ChatWorkTone = waitingForHuman
    ? 'attention'
    : isWorking
      ? 'working'
      : 'neutral';

  return {
    chatKey: selection.chatKey,
    identity: selection.identity,
    workAvailable: selection.availability === 'available',
    items: selection.items,
    events: selection.events,
    presence: selection.presence,
    bounds: selection.bounds,
    notices: selection.notices,
    coworker: {
      samplePresent: nativeSample !== null && nativeSample !== undefined,
      availability: coworkerAvailability,
      activity: coworkerActivity,
      freshness: coworkerFreshness,
      live: coworkerLive,
      waitingForHuman: coworkerWaiting,
    },
    agents,
    workingAgentCount,
    isWorking,
    waitingForHuman,
    statusLabel,
    statusTone,
  };
}

/**
 * Builds one exact-identity liveness entry per discovered chat. Native samples
 * without a chat_work entry remain visible as Coworker state; chat_work without
 * a native sample keeps durable backlog but no inferred Working/NeedsYou.
 */
export function buildChatWorkLivenessIndex(
  chatWork: unknown,
  nativeSamples: unknown,
  options: ChatWorkLivenessOptions,
): Map<string, ChatWorkLiveness> {
  const workByKey = new Map<string, ChatWorkCandidate>();
  for (const work of chatWorkCandidates(chatWork)) {
    const key = formatWorkspaceChatIdentityKey(work.identity);
    if (key !== null && !workByKey.has(key)) {
      workByKey.set(key, work);
    }
  }

  const sampleByKey = new Map<string, WorkspaceNativeSample>();
  for (const sample of nativeSampleCandidates(nativeSamples)) {
    const key = formatWorkspaceChatIdentityKey(sample.chat_identity);
    if (key !== null && !sampleByKey.has(key)) {
      sampleByKey.set(key, sample);
    }
  }

  const identities = new Map<string, WorkspaceChatIdentity>();
  for (const [key, work] of workByKey) {
    identities.set(key, work.identity);
  }
  for (const [key, sample] of sampleByKey) {
    if (!identities.has(key)) {
      identities.set(key, sample.chat_identity);
    }
  }

  const index = new Map<string, ChatWorkLiveness>();
  const orderedKeys = Array.from(identities.keys()).sort();
  for (const key of orderedKeys) {
    const identity = identities.get(key);
    if (!identity) {
      continue;
    }
    const work = workByKey.get(key);
    const selection =
      work !== undefined
        ? buildChatWorkSelectionFromWork(identity, work)
        : emptyChatWorkSelection(
            identity,
            'No backlog data is recorded for this chat yet.',
          );
    index.set(
      key,
      evaluateChatWorkLiveness(selection, sampleByKey.get(key) ?? null, options),
    );
  }
  return index;
}

export interface ChatWorkLivenessSummary {
  workingAgentCount: number;
  workingChatCount: number;
  waitingChatCount: number;
}

export function summarizeChatWorkLiveness(
  index: ReadonlyMap<string, ChatWorkLiveness>,
  chatKeys?: readonly string[],
): ChatWorkLivenessSummary {
  const keys = chatKeys !== undefined ? chatKeys : Array.from(index.keys());
  let workingAgentCount = 0;
  let workingChatCount = 0;
  let waitingChatCount = 0;
  for (const key of keys) {
    const entry = index.get(key);
    if (!entry) {
      continue;
    }
    workingAgentCount += entry.workingAgentCount;
    if (entry.isWorking) {
      workingChatCount += 1;
    }
    if (entry.waitingForHuman) {
      waitingChatCount += 1;
    }
  }
  return { workingAgentCount, workingChatCount, waitingChatCount };
}

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

interface OrderedScrollSyncResult {
  readonly state: MessageScrollState;
  readonly action: MessageScrollAction;
}

/**
 * Shared anchor-preserving reducer for an already-ordered stable item-id list.
 * Message records and chat-work event views both use this implementation so
 * append, head-trimming, follow/reading, and new-event behavior stay identical.
 */
function syncOrderedScrollItemIds(
  state: MessageScrollState,
  nextOrderedIds: readonly string[],
): OrderedScrollSyncResult {
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
      action: nextOrderedIds.length > 0 ? { kind: 'scroll_to_bottom' } : { kind: 'none' },
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
  const sync = syncOrderedScrollItemIds(
    state,
    orderedItems.map((item) => item.item_id),
  );
  return {
    state: sync.state,
    orderedItems,
    action: sync.action,
  };
}

export interface ChatWorkEventScrollSyncResult {
  readonly state: MessageScrollState;
  readonly action: MessageScrollAction;
}

/**
 * Reconciles the minimal chat-work stream's chronological event views with the
 * same tested message anchor reducer. `eventId` is the stable visible identity;
 * appends and head-trimming never replace it with a viewport `scrollTop`.
 */
export function syncChatWorkEventScroll(
  state: MessageScrollState,
  events: readonly Pick<ChatWorkEventView, 'eventId'>[],
): ChatWorkEventScrollSyncResult {
  const seenIds = new Set<string>();
  const orderedEventIds: string[] = [];
  for (const event of events) {
    const eventId = event.eventId;
    if (typeof eventId !== 'string' || eventId.trim().length === 0 || seenIds.has(eventId)) {
      continue;
    }
    seenIds.add(eventId);
    orderedEventIds.push(eventId);
  }
  return syncOrderedScrollItemIds(state, orderedEventIds);
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
