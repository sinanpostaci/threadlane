import {
  classifyTaskBucket,
  formatBindingLabel,
  formatProviderName,
  formatRelativeTimestamp,
  formatStatusLabel,
  normalizeStatusKey,
} from '../format.ts';
import type { DesktopSnapshot, TaskSummary } from '../types.ts';
import { h } from '../ui/dom.ts';



export const MAX_VISIBLE_RECOVERY_ITEMS = 4;
export const MAX_RECOVERY_SUMMARY_CHARS = 220;

export type RecoveryItemKind = 'unknown' | 'waiting' | 'review' | 'failed' | 'active';
export type RecoveryTone = 'accent' | 'warn' | 'danger';
export type RecoveryOverviewState = 'no_binding' | 'loading' | 'empty' | 'ready';

export interface RecoveryClassification {
  kind: RecoveryItemKind;
  kindLabel: string;
  tone: RecoveryTone;
  statusLabel: string;
  detail: string;
  nextStep: string;
  summary: string | null;
}

export interface RecoveryItem extends RecoveryClassification {
  task: TaskSummary;
  isDemo: boolean;
  inspectable: boolean;
}

export interface RecoveryProjection {
  state: RecoveryOverviewState;
  stale: boolean;
  inspectEnabled: boolean;
  statusLabel: string;
  statusNote: string;
  bindingId: string | null;
  bindingLabel: string | null;
  isDemoScope: boolean;
  tasksInScope: number;
  totalRuns: number;
  hasMore: boolean;
  coverageBounded: boolean;
  bounded: boolean;
  coverageNote: string;
  items: RecoveryItem[];
  visibleItems: RecoveryItem[];
  hiddenItemCount: number;
  attentionCount: number;
  activeCount: number;
  headline: string;
}

export interface RecoveryProjectionOptions {
  loading?: boolean;
  stale?: boolean;
  maxVisible?: number;
}

export interface RecoveryOverviewProps {
  snapshot: DesktopSnapshot;
  loading: boolean;
  stale: boolean;
  onOpenRun: (task: TaskSummary) => void;
}

const UNKNOWN_STATUS_KEYS = new Set(['unknown', 'uncertain']);
const REVIEW_STATUS_KEYS = new Set(['ready_for_review', 'pending_review']);
const FAILED_STATUS_KEYS = new Set([
  'failed',
  'execution_failed',
  'timed_out',
  'timedout',
  'timeout',
  'cancelled',
  'canceled',
]);
const ACTIVE_STATUS_KEYS = new Set([
  'queued',
  'claimed',
  'running',
  'completing',
  'cancel_requested',
]);

const KIND_PRIORITY: Readonly<Record<RecoveryItemKind, number>> = {
  unknown: 0,
  waiting: 1,
  review: 2,
  failed: 3,
  active: 4,
};

export function compactRecoveryText(
  value: string | null | undefined,
  maxLength: number = MAX_RECOVERY_SUMMARY_CHARS
): string | null {
  const collapsed = (value ?? '').replace(/\s+/g, ' ').trim();
  if (!collapsed) {
    return null;
  }
  const limit =
    Number.isFinite(maxLength) && maxLength >= 1
      ? Math.trunc(maxLength)
      : MAX_RECOVERY_SUMMARY_CHARS;
  if (collapsed.length <= limit) {
    return collapsed;
  }
  return `${collapsed.slice(0, limit - 1)}…`;
}

function activeKindLabel(normalized: string, statusLabel: string): string {
  switch (normalized) {
    case 'running':
      return 'Still running';
    case 'queued':
      return 'Queued';
    case 'claimed':
      return 'Claimed';
    case 'completing':
      return 'Completing';
    case 'cancel_requested':
      return 'Cancel requested';
    default:
      return statusLabel;
  }
}

/**
 * Pure attention classification for one snapshot task.
 *
 * It uses only recorded status, the explicit evidence flag, and the kind. It
 * never infers failure or completion from elapsed time.
 */
export function classifyRecoveryTask(task: TaskSummary): RecoveryClassification | null {
  const normalized = normalizeStatusKey(task.status);
  const statusLabel = formatStatusLabel(task.status);
  const summary = compactRecoveryText(task.last_summary);
  const isDemo = task.kind === 'demo';
  const subject = isDemo ? 'local demo run' : task.kind === 'worker' ? 'worker' : 'run';

  if (UNKNOWN_STATUS_KEYS.has(normalized)) {
    return {
      kind: 'unknown',
      kindLabel: isDemo ? 'Uncertain demo outcome' : 'Uncertain outcome',
      tone: 'warn',
      statusLabel,
      detail: `The last recorded outcome for this ${subject} is unknown. That is not proof of failure or completion.`,
      nextStep:
        'Inspect the run and reconcile its recorded state. Do not retry or resume automatically.',
      summary,
    };
  }

  if (isDemo) {
    if (classifyTaskBucket(task) === 'waiting') {
      return {
        kind: 'waiting',
        kindLabel: 'Local demo waiting',
        tone: 'warn',
        statusLabel,
        detail:
          'The local demo is waiting for input. The last recorded summary may contain the pending question or confirmation.',
        nextStep:
          'Inspect the run, then answer through the existing local demo controls or the linked conversation. This card does not send an answer.',
        summary,
      };
    }
    if (ACTIVE_STATUS_KEYS.has(normalized)) {
      return {
        kind: 'active',
        kindLabel: 'Local demo active',
        tone: 'accent',
        statusLabel,
        detail: `The local demo snapshot reports ${statusLabel.toLowerCase()}. This is a known active state; elapsed time alone is not treated as a failure.`,
        nextStep:
          'Inspect the run for recorded details. This card does not control the local demo.',
        summary,
      };
    }
    return null;
  }

  if (classifyTaskBucket(task) === 'waiting') {
    return {
      kind: 'waiting',
      kindLabel: 'Waiting for input',
      tone: 'warn',
      statusLabel,
      detail:
        'The task is waiting for input. The last recorded summary may contain the pending question.',
      nextStep:
        'Inspect the run, then answer in the linked Codex conversation. This card does not send an answer.',
      summary,
    };
  }

  if (ACTIVE_STATUS_KEYS.has(normalized)) {
    return {
      kind: 'active',
      kindLabel: activeKindLabel(normalized, statusLabel),
      tone: 'accent',
      statusLabel,
      detail: `The snapshot reports ${statusLabel.toLowerCase()}. This is a known active state; elapsed time alone is not treated as a failure.`,
      nextStep:
        'Inspect the run for recorded details. This card does not mark the run as stopped or restart it.',
      summary,
    };
  }

  if (FAILED_STATUS_KEYS.has(normalized)) {
    return {
      kind: 'failed',
      kindLabel: 'Execution ended',
      tone: 'danger',
      statusLabel,
      detail: `The recorded execution outcome is ${statusLabel}. This is a known terminal execution state, not an unknown outcome.`,
      nextStep:
        'Inspect the run to review the recorded outcome and evidence. This card does not retry, resume, or restart work.',
      summary,
    };
  }

  if (task.needs_evidence || REVIEW_STATUS_KEYS.has(normalized)) {
    return {
      kind: 'review',
      kindLabel: task.needs_evidence ? 'Evidence review' : 'Review pending',
      tone: 'warn',
      statusLabel,
      detail: task.needs_evidence
        ? `Recorded execution status: ${statusLabel}. Missing model or effort evidence is review work; the run is not accepted as a success.`
        : `Recorded execution status: ${statusLabel}. The run is waiting for review; completion is not acceptance.`,
      nextStep:
        'Inspect the run and review the recorded outcome and evidence. Do not treat completion as acceptance, and do not merge automatically.',
      summary,
    };
  }

  if (normalized === 'completed') {
    return null;
  }

  return {
    kind: 'unknown',
    kindLabel: 'Unrecognized status',
    tone: 'warn',
    statusLabel,
    detail: `The snapshot reported an unrecognized status for this ${subject}. Inspect the run before relying on it.`,
    nextStep:
      'Inspect the run and reconcile its recorded state. Do not retry or resume automatically.',
    summary,
  };
}

export function resolveRecoveryBindingId(snapshot: DesktopSnapshot): string | null {
  const raw = snapshot.selected_binding_id;
  return typeof raw === 'string' && raw.trim().length > 0 ? raw.trim() : null;
}

/** Returns only tasks already present in the snapshot for the selected binding. */
export function selectScopedRecoveryTasks(snapshot: DesktopSnapshot): TaskSummary[] {
  const bindingId = resolveRecoveryBindingId(snapshot);
  if (!bindingId) {
    return [];
  }
  return snapshot.tasks.filter((task) => task.binding_id === bindingId);
}

export function isRecoverySnapshotStale(
  snapshot: DesktopSnapshot,
  staleOverride: boolean = false
): boolean {
  return (
    staleOverride ||
    snapshot.attachment.connection !== 'connected' ||
    snapshot.attachment.mode === 'unconfigured'
  );
}

function resolveRecoveryFreshness(
  snapshot: DesktopSnapshot,
  staleOverride: boolean,
  loading: boolean
): { stale: boolean; statusLabel: string; statusNote: string } {
  const stale = isRecoverySnapshotStale(snapshot, staleOverride);
  if (stale) {
    let reason = 'This snapshot is marked stale.';
    if (!staleOverride) {
      switch (snapshot.attachment.connection) {
        case 'disconnected':
          reason = 'The runtime observer is disconnected.';
          break;
        case 'blocked':
          reason = 'Observer access is blocked.';
          break;
        case 'unconfigured':
          reason = 'No runtime is attached.';
          break;
        default:
          reason = 'The runtime is not reporting a connected state.';
          break;
      }
    }
    const suffix =
      snapshot.attachment.connection === 'disconnected'
        ? ' (offline)'
        : snapshot.attachment.connection === 'blocked'
          ? ' (blocked)'
          : '';
    return {
      stale: true,
      statusLabel: `Stale snapshot${suffix}`,
      statusNote: `${reason} Last known status is shown; it is not live. Inspect is disabled until a fresh snapshot arrives.`,
    };
  }

  if (loading) {
    return {
      stale: false,
      statusLabel: 'Refreshing snapshot',
      statusNote:
        'Refreshing the read-only snapshot. Shown status is from the previous snapshot, not a live stream.',
    };
  }

  return {
    stale: false,
    statusLabel: 'Read-only snapshot',
    statusNote:
      'Status comes from the last completed snapshot refresh, not a live process check.',
  };
}

function buildCoverageNote(
  bindingId: string | null,
  loading: boolean,
  tasksInScope: number,
  totalRuns: number,
  hasMore: boolean
): string {
  if (!bindingId) {
    return 'No conversation is selected, so there is no scoped recovery data to show.';
  }
  if (loading && tasksInScope === 0) {
    return 'Recent runs will be summarized when the snapshot loads.';
  }
  const shown = tasksInScope;
  if (hasMore || totalRuns > shown) {
    const totalText = totalRuns > 0 ? ` of ${totalRuns} recorded` : '';
    const moreText = hasMore
      ? ' More may exist beyond this snapshot.'
      : ' More may exist.';
    return `Recent runs only: showing ${shown}${totalText}.${moreText} Activity history is the place to inspect older runs.`;
  }
  if (shown === 0) {
    return 'No recent runs are included in this snapshot.';
  }
  return `Showing all ${shown} recent ${shown === 1 ? 'run' : 'runs'} included in this snapshot.`;
}

function normalizeMaxVisible(raw: number | undefined): number {
  if (typeof raw !== 'number' || !Number.isFinite(raw) || raw < 1) {
    return MAX_VISIBLE_RECOVERY_ITEMS;
  }
  return Math.max(1, Math.trunc(raw));
}

/**
 * Pure projection of recovery/attention items from one already-scoped snapshot.
 *
 * It deliberately does not derive tasks from aggregate counters and never
 * mutates the snapshot, application state, or backend state.
 */
export function projectRecoveryOverview(
  snapshot: DesktopSnapshot,
  options: RecoveryProjectionOptions = {}
): RecoveryProjection {
  const loading = options.loading === true;
  const bindingId = resolveRecoveryBindingId(snapshot);
  const binding = bindingId
    ? snapshot.bindings.find((candidate) => candidate.binding_id === bindingId) ?? null
    : null;
  const tasksInScope = selectScopedRecoveryTasks(snapshot);
  const isDemoScope =
    binding?.is_local_demo === true ||
    snapshot.attachment.mode === 'demo' ||
    (tasksInScope.length > 0 && tasksInScope.every((task) => task.kind === 'demo'));
  const freshness = resolveRecoveryFreshness(snapshot, options.stale === true, loading);

  const reportedTotal = Number.isFinite(snapshot.total_runs)
    ? Math.max(0, Math.trunc(snapshot.total_runs))
    : 0;
  const totalRuns = bindingId ? Math.max(reportedTotal, tasksInScope.length) : 0;
  const hasMore = bindingId !== null && snapshot.has_more === true;
  const coverageBounded = bindingId !== null && (hasMore || totalRuns > tasksInScope.length);
  const coverageNote = buildCoverageNote(
    bindingId,
    loading && !freshness.stale,
    tasksInScope.length,
    totalRuns,
    hasMore
  );

  const items: RecoveryItem[] = [];
  const seen = new Set<string>();
  for (const task of tasksInScope) {
    const classification = classifyRecoveryTask(task);
    if (!classification) {
      continue;
    }
    const key = `${task.binding_id}\u0000${task.run_id}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    items.push({
      ...classification,
      task,
      isDemo: task.kind === 'demo',
      inspectable: !freshness.stale,
    });
  }

  items.sort((left, right) => {
    const priority = KIND_PRIORITY[left.kind] - KIND_PRIORITY[right.kind];
    if (priority !== 0) {
      return priority;
    }
    const updated = right.task.updated_at_ms - left.task.updated_at_ms;
    if (updated !== 0) {
      return updated;
    }
    return left.task.run_id.localeCompare(right.task.run_id);
  });

  const maxVisible = normalizeMaxVisible(options.maxVisible);
  const visibleItems = items.slice(0, maxVisible);
  const hiddenItemCount = items.length - visibleItems.length;
  const attentionCount = items.filter((item) => item.kind !== 'active').length;
  const activeCount = items.length - attentionCount;

  let state: RecoveryOverviewState;
  if (!bindingId) {
    state = 'no_binding';
  } else if (loading && !freshness.stale && tasksInScope.length === 0) {
    state = 'loading';
  } else if (items.length === 0) {
    state = 'empty';
  } else {
    state = 'ready';
  }

  let headline: string;
  if (state === 'no_binding') {
    headline = 'No conversation selected';
  } else if (state === 'loading') {
    headline = 'Checking recent runs';
  } else if (items.length === 0) {
    headline = 'No recovery items in recent runs';
  } else if (attentionCount > 0) {
    headline = `${attentionCount} ${attentionCount === 1 ? 'item needs' : 'items need'} attention`;
  } else {
    headline = `${activeCount} ${activeCount === 1 ? 'run is' : 'runs are'} still active in this snapshot`;
  }

  return {
    state,
    stale: freshness.stale,
    inspectEnabled: !freshness.stale,
    statusLabel: freshness.statusLabel,
    statusNote: freshness.statusNote,
    bindingId,
    bindingLabel: binding ? formatBindingLabel(binding) : bindingId,
    isDemoScope,
    tasksInScope: tasksInScope.length,
    totalRuns,
    hasMore,
    coverageBounded,
    bounded: coverageBounded || hiddenItemCount > 0,
    coverageNote,
    items,
    visibleItems,
    hiddenItemCount,
    attentionCount,
    activeCount,
    headline,
  };
}

function formatRecoveryCount(projection: RecoveryProjection): string {
  if (projection.items.length === 0) {
    return 'No entries';
  }
  if (projection.hiddenItemCount > 0) {
    return `${projection.visibleItems.length} of ${projection.items.length}`;
  }
  return `${projection.items.length} ${projection.items.length === 1 ? 'item' : 'items'}`;
}

function buildRecoveryHeader(
  projection: RecoveryProjection,
  collapsible: boolean
): HTMLElement {
  const countTone =
    projection.attentionCount > 0
      ? 'warn'
      : projection.items.length > 0
        ? 'active'
        : 'default';
  return h(
    'span',
    { className: 'aq-recovery-header' },
    h(
      'span',
      { className: 'aq-recovery-heading' },
      h('span', { className: 'aq-recovery-title' }, 'Needs attention'),
      h(
        'span',
        { className: 'aq-recovery-scope' },
        projection.bindingLabel
          ? `Scoped to ${projection.bindingLabel}`
          : 'No conversation scope'
      )
    ),
    h(
      'span',
      { className: 'aq-recovery-meta' },
      h(
        'span',
        {
          className: 'aq-recovery-snapshot-badge',
          'data-state': projection.stale ? 'stale' : 'current',
        },
        projection.statusLabel
      ),
      h('span', { className: 'aq-badge', 'data-status': countTone }, formatRecoveryCount(projection)),
      collapsible
        ? h(
            'span',
            { className: 'aq-recovery-chevron', 'aria-hidden': 'true' },
            '▾'
          )
        : null
    )
  );
}

function buildRecoveryItem(
  entry: RecoveryItem,
  onOpenRun: (task: TaskSummary) => void
): HTMLElement {
  const inspectButton = h(
    'button',
    {
      type: 'button',
      className: 'aq-btn aq-recovery-inspect',
      disabled: !entry.inspectable,
      'aria-label': `Inspect run ${entry.task.run_id}`,
      'data-focus': `recovery-${entry.task.run_id}`,
    },
    'Inspect run'
  );
  inspectButton.addEventListener('click', () => {
    if (!inspectButton.disabled) {
      onOpenRun(entry.task);
    }
  });

  const provider = formatProviderName(entry.task);
  const metaParts = [provider];
  if (entry.isDemo && !provider.toLowerCase().includes('demo')) {
    metaParts.push('Local demo');
  }
  metaParts.push(`rev ${entry.task.revision}`);
  const statusText = entry.inspectable
    ? entry.statusLabel
    : `Last known: ${entry.statusLabel}`;

  return h(
    'li',
    {
      className: 'aq-recovery-item',
      'data-kind': entry.kind,
      'data-tone': entry.tone,
    },
    h(
      'div',
      { className: 'aq-recovery-item-head' },
      h('span', { className: 'aq-recovery-kind', 'data-tone': entry.tone }, entry.kindLabel),
      h('span', { className: 'aq-recovery-status' }, statusText),
      h(
        'span',
        { className: 'aq-recovery-time' },
        formatRelativeTimestamp(entry.task.updated_at_ms)
      )
    ),
    h('p', { className: 'aq-recovery-item-title' }, entry.task.title),
    h('p', { className: 'aq-recovery-item-meta' }, metaParts.join(' · ')),
    entry.summary
      ? h('p', { className: 'aq-recovery-summary' }, 'Last recorded: ', entry.summary)
      : null,
    h('p', { className: 'aq-recovery-detail' }, entry.detail),
    h('p', { className: 'aq-recovery-next' }, entry.nextStep),
    h(
      'div',
      { className: 'aq-recovery-item-actions' },
      inspectButton,
      entry.inspectable
        ? null
        : h(
            'span',
            { className: 'aq-recovery-disabled-note' },
            'Inspect is disabled while this snapshot is stale.'
          )
    )
  );
}

function buildRecoveryBody(
  projection: RecoveryProjection,
  onOpenRun: (task: TaskSummary) => void
): HTMLElement {
  const children: HTMLElement[] = [
    h('p', { className: 'aq-recovery-headline' }, projection.headline),
    h(
      'p',
      { className: 'aq-recovery-intro' },
      projection.isDemoScope
        ? 'Read-only local demo. No start, answer, retry, approval, merge, or settings change.'
        : 'Read-only. No start, retry, resume, approval, merge, or settings change.'
    ),
    h('p', { className: 'aq-recovery-coverage' }, projection.coverageNote),
  ];

  children.push(
    h(
      'p',
      {
        className: 'aq-recovery-snapshot-note',
        'data-state': projection.stale ? 'stale' : 'current',
      },
      projection.statusNote
    )
  );

  if (projection.visibleItems.length > 0) {
    children.push(
      h(
        'ul',
        { className: 'aq-recovery-list' },
        ...projection.visibleItems.map((entry) => buildRecoveryItem(entry, onOpenRun))
      )
    );
  } else {
    children.push(
      h(
        'p',
        { className: 'aq-recovery-empty' },
        'No recent run entries are available in this stale snapshot. Reconnect before relying on status.'
      )
    );
  }

  if (projection.hiddenItemCount > 0) {
    children.push(
      h(
        'p',
        { className: 'aq-recovery-hidden' },
        `${projection.hiddenItemCount} more ${
          projection.hiddenItemCount === 1 ? 'item is' : 'items are'
        } not shown in this compact card. Inspect them from the run list or Activity history.`
      )
    );
  }

  return h('div', { className: 'aq-recovery-body' }, ...children);
}

/**
 * Read-only recovery/attention card for the Activity page.
 *
 * The only action is the existing run-detail navigation callback. No state
 * mutation, worker control, approval, retry, acknowledgement, or settings path
 * exists here. Returns null when there is no scoped conversation or nothing to
 * show in a non-stale snapshot.
 */
export function renderRecoveryOverview({
  snapshot,
  loading,
  stale,
  onOpenRun,
}: RecoveryOverviewProps): HTMLElement | null {
  const projection = projectRecoveryOverview(snapshot, { loading, stale });
  if (projection.state === 'no_binding') {
    return null;
  }
  if (projection.attentionCount === 0 && !projection.stale) {
    return null;
  }

  const collapsible = projection.items.length > 1;
  const header = buildRecoveryHeader(projection, collapsible);
  const body = buildRecoveryBody(projection, onOpenRun);
  const attributes = {
    className: 'aq-recovery-card',
    'data-recovery-overview': 'true',
    'data-state': projection.state,
    'data-stale': projection.stale ? 'true' : 'false',
    'aria-label': 'Recovery and attention overview',
  };

  if (collapsible) {
    return h(
      'details',
      { ...attributes },
      h('summary', { className: 'aq-recovery-summary-row' }, header),
      body
    );
  }

  return h('section', attributes, header, body);
}
