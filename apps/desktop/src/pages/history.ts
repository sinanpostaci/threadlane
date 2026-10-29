import {
  EVENT_KINDS,
  type ActivityEvent,
  type Attachment,
  type DesktopActivityHistoryArgs,
  type EventKind,
} from '../types.ts';
import { h } from '../ui/dom.ts';

export const MAX_RETAINED_HISTORY_EVENTS = 200;
export const DEFAULT_HISTORY_PAGE_LIMIT = 20;

export type HistoryLoadingMode = 'initial' | 'more' | 'refresh' | 'poll' | null;

export interface HistoryQueryTicket {
  readonly seq: number;
  readonly queryEpoch: number;
  readonly scopeKey: string;
  readonly bindingId: string;
  readonly afterSeq: number | null;
  readonly cursor: string | null;
  readonly limit: number;
  readonly runId: string | null;
  readonly kind: EventKind | null;
  readonly mode: Exclude<HistoryLoadingMode, null>;
}

export interface HistorySubViewState {
  expanded: boolean;
  runIdDraft: string;
  runIdFilter: string | null;
  kindFilter: EventKind | null;
  queryEpoch: number;
  scopeKey: string;
  requestSeq: number;
  inFlight: boolean;
  loadingMode: HistoryLoadingMode;
  events: ActivityEvent[];
  nextCursor: string | null;
  hasMore: boolean;
  highWaterMarkSeq: number;
  completedHighWaterSeq: number;
  lastAfterSeq: number;
  error: string | null;
  selectedEventId: string | null;
  hasLoadedOnce: boolean;
}

const EVENT_KIND_LABELS: Record<EventKind, string> = {
  run_started: 'Run started',
  run_progress: 'Run progress',
  question_asked: 'Question asked',
  question_answered: 'Question answered',
  question_superseded: 'Question superseded',
  run_steered: 'Run steered',
  run_completed: 'Run completed',
  run_cancelled: 'Run cancelled',
  worker_queued: 'Worker queued',
  worker_started: 'Worker started',
  worker_progress: 'Worker progress',
  worker_cancel_requested: 'Cancel requested',
  worker_completed: 'Worker completed',
  worker_failed: 'Worker failed',
  worker_cancelled: 'Worker cancelled',
  worker_timed_out: 'Worker timed out',
  worker_unknown: 'Worker unknown',
};

export function isValidEventKind(value: string | null | undefined): value is EventKind {
  if (!value) {
    return false;
  }
  return (EVENT_KINDS as readonly string[]).includes(value);
}

export function formatEventKindLabel(kind: EventKind): string {
  return EVENT_KIND_LABELS[kind] ?? kind.replace(/_/g, ' ');
}

export function resolveEventKindTone(
  kind: EventKind
): 'neutral' | 'active' | 'warn' | 'danger' | 'ok' {
  switch (kind) {
    case 'run_completed':
    case 'worker_completed':
    case 'question_answered':
      return 'ok';
    case 'question_asked':
    case 'worker_cancel_requested':
    case 'question_superseded':
      return 'warn';
    case 'worker_failed':
    case 'worker_timed_out':
    case 'worker_unknown':
    case 'run_cancelled':
    case 'worker_cancelled':
      return 'danger';
    case 'run_started':
    case 'run_progress':
    case 'run_steered':
    case 'worker_queued':
    case 'worker_started':
    case 'worker_progress':
      return 'active';
    default:
      return 'neutral';
  }
}

export function formatHistoryTimestamp(timestampMs: number): string {
  if (!Number.isFinite(timestampMs) || timestampMs <= 0) {
    return '--:--:--';
  }
  const date = new Date(timestampMs);
  const hh = String(date.getHours()).padStart(2, '0');
  const mm = String(date.getMinutes()).padStart(2, '0');
  const ss = String(date.getSeconds()).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

export function computeHistoryScopeKey(
  attachment: Pick<Attachment, 'mode' | 'state_dir'>,
  selectedBindingId: string | null,
  runIdFilter: string | null,
  kindFilter: EventKind | null
): string {
  const mode = attachment.mode ?? 'unconfigured';
  const dir = attachment.state_dir?.trim() ?? '';
  const binding = selectedBindingId?.trim() ?? '';
  const runId = runIdFilter?.trim() ?? '';
  const kind = kindFilter ?? '';
  return `${mode}|${dir}|${binding}|${runId}|${kind}`;
}

export function createInitialHistoryState(
  attachment: Pick<Attachment, 'mode' | 'state_dir'> = {
    mode: 'unconfigured',
    state_dir: null,
  },
  selectedBindingId: string | null = null
): HistorySubViewState {
  return {
    expanded: false,
    runIdDraft: '',
    runIdFilter: null,
    kindFilter: null,
    queryEpoch: 1,
    scopeKey: computeHistoryScopeKey(attachment, selectedBindingId, null, null),
    requestSeq: 0,
    inFlight: false,
    loadingMode: null,
    events: [],
    nextCursor: null,
    hasMore: false,
    highWaterMarkSeq: 0,
    completedHighWaterSeq: 0,
    lastAfterSeq: 0,
    error: null,
    selectedEventId: null,
    hasLoadedOnce: false,
  };
}

export function syncHistoryScope(
  history: HistorySubViewState,
  attachment: Pick<Attachment, 'mode' | 'state_dir'>,
  selectedBindingId: string | null
): HistorySubViewState {
  const nextScopeKey = computeHistoryScopeKey(
    attachment,
    selectedBindingId,
    history.runIdFilter,
    history.kindFilter
  );
  if (nextScopeKey === history.scopeKey) {
    return history;
  }
  return {
    ...history,
    queryEpoch: history.queryEpoch + 1,
    scopeKey: nextScopeKey,
    requestSeq: history.requestSeq + 1,
    inFlight: false,
    loadingMode: null,
    events: [],
    nextCursor: null,
    hasMore: false,
    highWaterMarkSeq: 0,
    completedHighWaterSeq: 0,
    lastAfterSeq: 0,
    error: null,
    selectedEventId: null,
    hasLoadedOnce: false,
  };
}

export function setHistoryRunIdDraft(
  history: HistorySubViewState,
  draft: string
): HistorySubViewState {
  if (history.runIdDraft === draft) {
    return history;
  }
  return {
    ...history,
    runIdDraft: draft,
  };
}

export function applyHistoryFilters(
  history: HistorySubViewState,
  attachment: Pick<Attachment, 'mode' | 'state_dir'>,
  selectedBindingId: string | null,
  runIdInput?: string | null,
  kindFilter?: EventKind | null
): HistorySubViewState {
  const rawRun = runIdInput !== undefined ? (runIdInput ?? '') : history.runIdDraft;
  const normalizedRun = rawRun.trim() ? rawRun.trim() : null;
  const normalizedKind = kindFilter !== undefined ? kindFilter : history.kindFilter;
  const nextScopeKey = computeHistoryScopeKey(
    attachment,
    selectedBindingId,
    normalizedRun,
    normalizedKind
  );

  if (
    nextScopeKey === history.scopeKey &&
    history.runIdFilter === normalizedRun &&
    history.kindFilter === normalizedKind
  ) {
    return {
      ...history,
      runIdDraft: normalizedRun ?? '',
    };
  }

  return {
    ...history,
    runIdDraft: normalizedRun ?? '',
    runIdFilter: normalizedRun,
    kindFilter: normalizedKind,
    queryEpoch: history.queryEpoch + 1,
    scopeKey: nextScopeKey,
    requestSeq: history.requestSeq + 1,
    inFlight: false,
    loadingMode: null,
    events: [],
    nextCursor: null,
    hasMore: false,
    highWaterMarkSeq: 0,
    completedHighWaterSeq: 0,
    lastAfterSeq: 0,
    error: null,
    selectedEventId: null,
    hasLoadedOnce: false,
  };
}

export function clearHistoryFilters(
  history: HistorySubViewState,
  attachment: Pick<Attachment, 'mode' | 'state_dir'>,
  selectedBindingId: string | null
): HistorySubViewState {
  return applyHistoryFilters(history, attachment, selectedBindingId, '', null);
}

export function toggleHistoryExpanded(
  history: HistorySubViewState,
  expanded?: boolean
): HistorySubViewState {
  const nextExpanded = expanded !== undefined ? expanded : !history.expanded;
  if (nextExpanded === history.expanded) {
    return history;
  }
  if (!nextExpanded) {
    return {
      ...history,
      expanded: false,
      inFlight: false,
      loadingMode: null,
      requestSeq: history.requestSeq + 1,
    };
  }
  return {
    ...history,
    expanded: true,
  };
}

export function toggleHistoryEventDetail(
  history: HistorySubViewState,
  eventId: string | null
): HistorySubViewState {
  const nextId = history.selectedEventId === eventId ? null : eventId;
  return {
    ...history,
    selectedEventId: nextId,
  };
}

/**
 * Determines the next keyset pagination parameters for `list_activity`.
 *
 * - If an anchored `next_cursor` is still unconsumed (`history.nextCursor !== null`),
 *   it MUST be drained first (`{ cursor: history.nextCursor, afterSeq: null }`) so polls
 *   and refreshes never skip unconsumed anchored pages.
 * - Once the anchored window is completely drained (`history.nextCursor === null`),
 *   subsequent refreshes and polls query fresh events after `completedHighWaterSeq`
 *   (`{ cursor: null, afterSeq: history.completedHighWaterSeq }`).
 */
export function resolveNextHistoryCursorOrAfterSeq(history: HistorySubViewState): {
  cursor: string | null;
  afterSeq: number | null;
} {
  if (history.nextCursor !== null) {
    return {
      cursor: history.nextCursor,
      afterSeq: null,
    };
  }
  return {
    cursor: null,
    afterSeq: history.completedHighWaterSeq,
  };
}

export function toActivityHistoryIpcArgs(
  ticket: HistoryQueryTicket
): DesktopActivityHistoryArgs {
  return {
    bindingId: ticket.bindingId,
    afterSeq: ticket.afterSeq,
    cursor: ticket.cursor,
    limit: ticket.limit,
    runId: ticket.runId,
    kind: ticket.kind,
  };
}

export function mergeAndCapHistoryEvents(
  existing: readonly ActivityEvent[],
  incoming: readonly ActivityEvent[],
  bindingId: string,
  maxRetained: number = MAX_RETAINED_HISTORY_EVENTS
): ActivityEvent[] {
  const bySeq = new Map<number, ActivityEvent>();
  for (const event of existing) {
    if (event.binding_id === bindingId) {
      bySeq.set(event.seq, event);
    }
  }
  for (const event of incoming) {
    if (event.binding_id === bindingId) {
      bySeq.set(event.seq, event);
    }
  }
  const merged = Array.from(bySeq.values()).sort((a, b) => a.seq - b.seq);
  const cap = Math.max(1, maxRetained);
  if (merged.length > cap) {
    return merged.slice(merged.length - cap);
  }
  return merged;
}

export interface HistorySectionCallbacks {
  onToggleExpanded: () => void;
  onRunIdDraftInput: (draft: string) => void;
  onApplyFilters: () => void;
  onClearFilters: () => void;
  onKindFilterChange: (kind: EventKind | null) => void;
  onLoadMore: () => void;
  onRefresh: () => void;
  onSelectEvent: (eventId: string) => void;
  onOpenRunDetail?: (bindingId: string, runId: string) => void;
}

export interface HistorySectionRenderProps {
  history: HistorySubViewState;
  attachment: Attachment;
  selectedBindingId: string | null;
  isDevPreviewFixture: boolean;
  hasTauriRuntime: boolean;
  callbacks: HistorySectionCallbacks;
}

function renderEventDetailPanel(
  event: ActivityEvent,
  onOpenRunDetail?: (bindingId: string, runId: string) => void
): HTMLElement {
  const rows: Array<[string, string]> = [
    ['Event ID', event.event_id],
    ['Sequence', `#${event.seq} · rev ${event.revision} · step ${event.step_seq}`],
    ['Run ID', event.run_id],
    ['Kind', `${formatEventKindLabel(event.kind)} (${event.kind})`],
    ['Important', event.important ? 'Yes (triggers wake-up)' : 'No'],
    ['Summary', event.payload.summary],
  ];

  if (event.payload.question_id) {
    rows.push(['Question ID', event.payload.question_id]);
  }
  if (event.payload.question_prompt) {
    rows.push(['Question prompt', event.payload.question_prompt]);
  }
  if (event.payload.answer) {
    rows.push(['Answer', event.payload.answer]);
  }
  if (event.payload.instruction) {
    rows.push(['Instruction', event.payload.instruction]);
  }
  if (event.payload.cancel_reason) {
    rows.push(['Cancel reason', event.payload.cancel_reason]);
  }
  rows.push([
    'Data boundary',
    event.payload.untrusted_data
      ? 'untrusted_data: true (plain-text display only)'
      : 'trusted metadata',
  ]);

  let detailActions: HTMLElement | null = null;
  if (onOpenRunDetail) {
    const openRunDetail = onOpenRunDetail;
    const openRunButton = h(
      'button',
      { type: 'button', className: 'aq-btn' },
      `Open run ${event.run_id}`
    );
    openRunButton.addEventListener('click', (clickEvent) => {
      clickEvent.stopPropagation();
      openRunDetail(event.binding_id, event.run_id);
    });
    detailActions = h(
      'div',
      { className: 'aq-history-detail-actions' },
      openRunButton
    );
  }

  return h(
    'div',
    { className: 'aq-history-detail' },
    h(
      'dl',
      { className: 'aq-history-detail-grid' },
      ...rows.map(([label, value]) =>
        h(
          'div',
          { className: 'aq-history-detail-row' },
          h('dt', {}, label),
          h('dd', {}, value)
        )
      )
    ),
    detailActions
  );
}

export function renderActivityHistorySection(
  props: HistorySectionRenderProps
): HTMLElement {
  const {
    history,
    attachment,
    selectedBindingId,
    isDevPreviewFixture,
    hasTauriRuntime,
    callbacks,
  } = props;

  const isBrowserDisconnected =
    !hasTauriRuntime &&
    !isDevPreviewFixture &&
    attachment.connection === 'disconnected';

  const hasActiveFilters =
    history.runIdFilter !== null || history.kindFilter !== null;
  const draftDiffersFromCommitted =
    history.runIdDraft.trim() !== (history.runIdFilter ?? '');

  const headerBadgeText = history.hasLoadedOnce
    ? `hwm #${history.highWaterMarkSeq} · ${history.events.length}${history.hasMore ? '+' : ''}`
    : 'read-only';

  const toggleBtn = h(
    'button',
    {
      type: 'button',
      className: 'aq-section-toggle aq-history-toggle',
      'aria-expanded': history.expanded ? 'true' : 'false',
      'data-history-toggle': 'true',
    },
    h(
      'span',
      { className: 'aq-history-toggle-title' },
      `${history.expanded ? '▾' : '▸'} History`
    ),
    h('span', { className: 'aq-history-toggle-meta' }, headerBadgeText)
  );
  toggleBtn.addEventListener('click', () => callbacks.onToggleExpanded());

  if (!history.expanded) {
    return h(
      'section',
      { className: 'aq-section aq-history-section' },
      toggleBtn
    );
  }

  const runFilterInput = h('input', {
    type: 'text',
    className: 'aq-input aq-history-run-input',
    placeholder: 'Filter run ID…',
    value: history.runIdDraft,
    'aria-label': 'Filter activity history by run ID',
    'data-focus': 'history-run-filter',
  });
  runFilterInput.addEventListener('input', (inputEvent) => {
    const target = inputEvent.currentTarget as HTMLInputElement;
    callbacks.onRunIdDraftInput(target.value);
  });
  runFilterInput.addEventListener('keydown', (keyEvent) => {
    if (keyEvent.key === 'Enter') {
      keyEvent.preventDefault();
      callbacks.onApplyFilters();
    }
  });

  const kindSelect = h(
    'select',
    {
      className: 'aq-select aq-history-kind-select',
      'aria-label': 'Filter activity history by event kind',
      'data-focus': 'history-kind-filter',
    },
    h(
      'option',
      { value: '', selected: history.kindFilter === null },
      'All kinds'
    ),
    ...EVENT_KINDS.map((kind) =>
      h(
        'option',
        { value: kind, selected: history.kindFilter === kind },
        formatEventKindLabel(kind)
      )
    )
  );
  kindSelect.addEventListener('change', (changeEvent) => {
    const selectedKind = (changeEvent.currentTarget as HTMLSelectElement).value;
    callbacks.onKindFilterChange(
      isValidEventKind(selectedKind) ? selectedKind : null
    );
  });

  const canQuery =
    Boolean(selectedBindingId) &&
    attachment.connection === 'connected' &&
    attachment.mode !== 'unconfigured';

  const canLoadMore =
    canQuery && !history.inFlight && history.hasMore && history.nextCursor !== null;

  const applyFilterButton = h(
    'button',
    {
      type: 'button',
      className: 'aq-btn',
      'data-history-apply-filter': 'true',
      disabled: !canQuery && !draftDiffersFromCommitted,
    },
    'Filter'
  );
  applyFilterButton.addEventListener('click', () => callbacks.onApplyFilters());

  let clearFilterButton: HTMLButtonElement | null = null;
  if (hasActiveFilters || history.runIdDraft.trim().length > 0) {
    clearFilterButton = h(
      'button',
      {
        type: 'button',
        className: 'aq-btn',
        'data-history-clear-filter': 'true',
      },
      'Clear'
    );
    clearFilterButton.addEventListener('click', () => callbacks.onClearFilters());
  }

  const loadMoreButton = h(
    'button',
    {
      type: 'button',
      className: 'aq-btn',
      'data-history-more': 'true',
      disabled: !canLoadMore,
    },
    history.inFlight && history.loadingMode === 'more'
      ? 'Loading…'
      : history.hasMore
        ? 'More'
        : 'No more'
  );
  loadMoreButton.addEventListener('click', () => callbacks.onLoadMore());

  const refreshButton = h(
    'button',
    {
      type: 'button',
      className: 'aq-btn',
      'data-history-refresh': 'true',
      disabled: !canQuery || history.inFlight,
    },
    history.inFlight && history.loadingMode !== 'more' ? 'Syncing…' : 'Refresh'
  );
  refreshButton.addEventListener('click', () => callbacks.onRefresh());

  const filterBar = h(
    'div',
    { className: 'aq-history-controls' },
    h(
      'div',
      { className: 'aq-history-filter-row' },
      runFilterInput,
      kindSelect,
      applyFilterButton,
      clearFilterButton
    ),
    h(
      'div',
      { className: 'aq-history-action-row' },
      h(
        'span',
        { className: 'aq-history-status-meta' },
        `hwm #${history.highWaterMarkSeq} · has_more: ${history.hasMore ? 'yes' : 'no'} · ${history.events.length}/${MAX_RETAINED_HISTORY_EVENTS}`
      ),
      h(
        'div',
        { className: 'aq-history-action-buttons' },
        loadMoreButton,
        refreshButton
      )
    )
  );

  let bodyNode: HTMLElement;

  if (isBrowserDisconnected) {
    bodyNode = h(
      'div',
      {
        className: 'aq-history-notice aq-history-notice--warn',
        'data-history-bridge-disconnected': 'true',
      },
      'bridge_disconnected: Tauri desktop bridge is not connected in this browser window. Launch inside Threadlane desktop or use DEV ?preview=1.'
    );
  } else if (attachment.connection !== 'connected' || !selectedBindingId) {
    bodyNode = h(
      'div',
      { className: 'aq-history-empty' },
      attachment.connection !== 'connected'
        ? 'Attach a live runtime or start a local demo to view activity history.'
        : 'Select a chat binding to inspect its activity history.'
    );
  } else if (history.events.length === 0) {
    const emptyText = history.inFlight
      ? 'Loading bounded activity history…'
      : hasActiveFilters
        ? `No activity events matched filters (high-water #${history.highWaterMarkSeq}).`
        : `No activity events recorded for this binding (high-water #${history.highWaterMarkSeq}).`;
    bodyNode = h('div', { className: 'aq-history-empty' }, emptyText);
  } else {
    bodyNode = h(
      'ul',
      { className: 'aq-history-list' },
      ...history.events.map((event) => {
        const isSelected = history.selectedEventId === event.event_id;
        const tone = resolveEventKindTone(event.kind);
        const rowButton = h(
          'button',
          {
            type: 'button',
            className: 'aq-history-row-btn',
            'aria-expanded': isSelected ? 'true' : 'false',
            'data-history-event-id': event.event_id,
          },
          h(
            'div',
            { className: 'aq-history-row-top' },
            h('span', { className: 'aq-history-seq' }, `#${event.seq}`),
            h(
              'span',
              { className: 'aq-history-time' },
              formatHistoryTimestamp(event.created_at_ms)
            ),
            h(
              'span',
              { className: `aq-history-kind aq-history-kind--${tone}` },
              formatEventKindLabel(event.kind)
            ),
            h('span', { className: 'aq-history-run' }, event.run_id)
          ),
          h(
            'div',
            { className: 'aq-history-summary' },
            event.payload.summary
          )
        );
        rowButton.addEventListener('click', () =>
          callbacks.onSelectEvent(event.event_id)
        );
        return h(
          'li',
          {
            className: `aq-history-item${isSelected ? ' is-selected' : ''}`,
          },
          rowButton,
          isSelected
            ? renderEventDetailPanel(event, callbacks.onOpenRunDetail)
            : null
        );
      })
    );
  }

  return h(
    'section',
    { className: 'aq-section aq-history-section' },
    toggleBtn,
    isDevPreviewFixture
      ? h(
          'div',
          {
            className: 'aq-history-notice aq-history-notice--preview',
            'data-history-preview-banner': 'true',
          },
          'DEV preview fixture (?preview=1) — synthetic sample, not live runtime history.'
        )
      : null,
    filterBar,
    history.error
      ? h(
          'div',
          { className: 'aq-history-notice aq-history-notice--danger' },
          history.error
        )
      : null,
    bodyNode
  );
}
