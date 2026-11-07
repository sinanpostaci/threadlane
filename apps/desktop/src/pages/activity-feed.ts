import { createBrandIconElement, createUiIcon } from '../icons.ts';
import { h } from '../ui/dom.ts';
import type { ObservationSnapshot } from '../observation/types.ts';
import {
  countRunningRequests,
  createActivityFeedFilters,
  formatActivityFilterCoverage,
  formatActivityRunningCountLabel,
  formatActivityScopeLabel,
  formatCompactDuration,
  formatCompactUsage,
  formatDurationMs,
  formatExecutorRoleLabel,
  formatFeedTimestamp,
  formatRelativeTime,
  formatRunningCountLabel,
  projectActivityFeed,
  projectRequestExecutorDisplay,
  projectVisibleTimeline,
  requestIdentityKey,
  resolveExecutorBrand,
  type ActivityFeedFilters,
  type ActivityFeedProjection,
  type ActivityFeedScope,
  type ActivityFilterOption,
  type ActivityRequestView,
  type ActivityVisibleTimelineEvent,
  type LifecycleTone,
  type ObservationLifecycleState,
} from './activity-feed-model.ts';

if (typeof window !== 'undefined' && typeof import.meta.glob === 'function') {
  import.meta.glob('./activity-feed.css', { eager: true });
}

interface ActivityFeedPageOptions {
  onViewJob?: (scope: { source_id: string; job_id: string }) => void;
  onLoadMore?: () => void;
}

interface ActivityFeedUpdateOptions {
  loading?: boolean;
  error?: string | null;
  scope?: ActivityFeedScope | null;
}

interface RenderedRow {
  key: string;
  li: HTMLLIElement;
  details: HTMLDetailsElement;
  summary: HTMLElement;
  signature: string;
  contextTimeEl: HTMLElement | null;
  durationEl: HTMLElement | null;
  contextAtMs: number | null;
  elapsedStartedAtMs: number | null;
  lastView: ActivityRequestView | null;
}

function recordedTimestampMs(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value > 0
    ? value
    : null;
}

function elapsedDurationTexts(view: ActivityRequestView): string[] {
  const texts: string[] = [];
  const collect = (
    duration: ActivityRequestView['duration'],
    summary: string,
    detail: string
  ): void => {
    if (duration.kind === 'elapsed') {
      // The compact value is handled by the duration object itself.
      texts.push(summary, detail);
    }
  };
  collect(view.duration, view.durationSummary, view.durationDetail);
  for (const child of view.children) {
    collect(child.duration, child.durationSummary, child.durationDetail);
  }
  return texts;
}

/**
 * Running elapsed labels change on every projection without changing any
 * recorded row content. Their exact generated strings are excluded from the
 * row-identity signature so the quotes and detail nodes stay stable.
 */
function activityRowSignature(
  view: ActivityRequestView,
  parentPresent: boolean,
  stale: boolean
): string {
  const elapsedTexts = new Set(elapsedDurationTexts(view));

  return JSON.stringify(
    [view, parentPresent, stale],
    (key, value) => {
      if (
        key === 'duration' &&
        typeof value === 'object' &&
        value !== null &&
        (value as { kind?: string }).kind === 'elapsed'
      ) {
        return { ...(value as ActivityRequestView['duration']), text: '' };
      }
      if (
        typeof value === 'string' &&
        (key === 'durationSummary' || key === 'durationDetail') &&
        elapsedTexts.has(value)
      ) {
        return '';
      }
      if (Array.isArray(value) && key === 'lines') {
        return value.map((line: unknown) =>
          typeof line === 'string' && elapsedTexts.has(line) ? '' : line
        );
      }
      return value;
    }
  );
}

function selectedFilterValue(select: HTMLSelectElement): string | null {
  const value = select.value.trim();
  return value.length > 0 ? value : null;
}

function buildFilterField(
  labelText: string,
  control: HTMLSelectElement
): HTMLElement {
  return h(
    'label',
    { className: 'aq-feed-filter-field' },
    h('span', { className: 'aq-feed-filter-label' }, labelText),
    control
  );
}

function filterOptionTitle(option: ActivityFilterOption): string {
  const count = `${option.count} loaded ${
    option.count === 1 ? 'request' : 'requests'
  } in this view`;
  return `${option.detail} · ${count}`;
}

type ActivityFilterDimension = 'projects' | 'chats' | 'states';

/**
 * Read-only durable request feed. The page only observes the supplied snapshot;
 * its sole actions are the optional View job and Load more callbacks.
 */
export class ActivityFeedPage {
  readonly element: HTMLElement;

  private readonly onViewJob: ((scope: { source_id: string; job_id: string }) => void) | null;
  private readonly onLoadMore: (() => void) | null;
  private readonly scopeEl: HTMLElement;
  private readonly runningCountEl: HTMLElement;
  private readonly allActivityButton: HTMLButtonElement;
  private readonly filtersButton: HTMLButtonElement;
  private readonly filterPanelEl: HTMLElement;
  private readonly filterSummaryEl: HTMLElement;
  private readonly filterStatusEl: HTMLElement;
  private readonly clearFiltersButton: HTMLButtonElement;
  private readonly projectFilterSelect: HTMLSelectElement;
  private readonly chatFilterSelect: HTMLSelectElement;
  private readonly stateFilterSelect: HTMLSelectElement;
  private readonly noticesEl: HTMLElement;
  private readonly listEl: HTMLUListElement;
  private readonly emptyEl: HTMLElement;
  private readonly footerEl: HTMLElement;
  private readonly footerNoteEl: HTMLElement;
  private readonly loadMoreButton: HTMLButtonElement;
  private readonly rows = new Map<string, RenderedRow>();
  private readonly expanded = new Set<string>();
  private readonly filterOptionCache: Record<
    ActivityFilterDimension,
    Map<string, ActivityFilterOption>
  > = {
    projects: new Map(),
    chats: new Map(),
    states: new Map(),
  };
  private readonly filterOptionSignatures: Record<
    ActivityFilterDimension,
    string
  > = {
    projects: '',
    chats: '',
    states: '',
  };
  private filters: ActivityFeedFilters = createActivityFeedFilters();
  private filtersOpen = false;
  private filtersAvailable = false;
  private activeFilterCount = 0;
  private snapshot: ObservationSnapshot | null = null;
  private scope: ActivityFeedScope | null = null;
  private loading = false;
  private error: string | null = null;
  private disposed = false;

  constructor(options: ActivityFeedPageOptions = {}) {
    this.onViewJob = options.onViewJob ?? null;
    this.onLoadMore = options.onLoadMore ?? null;

    this.scopeEl = h('p', { className: 'aq-feed-scope', 'data-feed-scope': 'true' });
    this.runningCountEl = h('span', {
      className: 'aq-feed-running-count',
      'aria-live': 'polite',
      hidden: true,
    });
    this.allActivityButton = h(
      'button',
      {
        type: 'button',
        className: 'aq-feed-all-activity tl-action',
        'data-action': 'activity-all',
        'data-focus-key': 'feed:all-activity',
        'aria-label': 'Show all activity',
        title: 'Show all activity',
        hidden: true,
      },
      'All activity'
    );
    this.filtersButton = h(
      'button',
      {
        type: 'button',
        className: 'aq-feed-filter-toggle tl-action',
        'data-action': 'activity-filters',
        'data-focus-key': 'feed:filter-toggle',
        'aria-expanded': 'false',
        'aria-controls': 'activity-feed-filters',
        'aria-label': 'Filters',
        title: 'Show activity filters',
        hidden: true,
      },
      'Filters'
    );
    this.projectFilterSelect = h('select', {
      className: 'aq-feed-filter-select',
      'data-focus-key': 'feed:filter-project',
      'aria-label': 'Filter loaded activity by project',
    });
    this.chatFilterSelect = h('select', {
      className: 'aq-feed-filter-select',
      'data-focus-key': 'feed:filter-chat',
      'aria-label': 'Filter loaded activity by chat',
    });
    this.stateFilterSelect = h('select', {
      className: 'aq-feed-filter-select',
      'data-focus-key': 'feed:filter-state',
      'aria-label': 'Filter loaded activity by lifecycle state',
    });
    this.filterStatusEl = h('p', {
      className: 'aq-feed-filter-status',
      id: 'activity-feed-filter-status',
      'aria-live': 'polite',
    });
    this.clearFiltersButton = h(
      'button',
      {
        type: 'button',
        className: 'aq-btn aq-feed-filter-clear',
        'data-focus-key': 'feed:filter-clear',
        disabled: true,
      },
      'Clear filters'
    );
    this.filterPanelEl = h(
      'div',
      {
        className: 'aq-feed-filters',
        id: 'activity-feed-filters',
        role: 'group',
        'aria-label': 'Activity filters',
        'aria-describedby': 'activity-feed-filter-status',
        hidden: true,
      },
      h(
        'div',
        { className: 'aq-feed-filter-grid' },
        buildFilterField('Project', this.projectFilterSelect),
        buildFilterField('Chat', this.chatFilterSelect),
        buildFilterField('State', this.stateFilterSelect)
      ),
      h(
        'div',
        { className: 'aq-feed-filter-actions' },
        this.filterStatusEl,
        this.clearFiltersButton
      )
    );
    this.filterSummaryEl = h('p', {
      className: 'aq-feed-filter-summary',
      'aria-live': 'polite',
      hidden: true,
    });
    this.noticesEl = h('div', { className: 'aq-feed-notices' });
    this.listEl = h('ul', {
      className: 'aq-feed-list',
      'aria-label': 'Recorded activity requests',
    });
    this.emptyEl = h('p', { className: 'aq-feed-empty', hidden: true });
    this.footerNoteEl = h('span', { className: 'aq-feed-footer-note', hidden: true });
    this.loadMoreButton = h(
      'button',
      {
        type: 'button',
        className: 'aq-btn aq-feed-load-more',
        'data-focus-key': 'feed:load-more',
        hidden: true,
      },
      'Load more'
    );
    this.loadMoreButton.addEventListener('click', () => {
      if (!this.loadMoreButton.disabled) {
        this.onLoadMore?.();
      }
    });
    this.filtersButton.addEventListener('click', () => {
      this.filtersOpen = !this.filtersOpen;
      this.syncFilterPanel();
    });
    this.projectFilterSelect.addEventListener('change', () => {
      this.applyFilters({
        ...this.filters,
        projectKey: selectedFilterValue(this.projectFilterSelect),
      });
    });
    this.chatFilterSelect.addEventListener('change', () => {
      this.applyFilters({
        ...this.filters,
        chatKey: selectedFilterValue(this.chatFilterSelect),
      });
    });
    this.stateFilterSelect.addEventListener('change', () => {
      this.applyFilters({
        ...this.filters,
        state: selectedFilterValue(
          this.stateFilterSelect
        ) as ObservationLifecycleState | null,
      });
    });
    this.clearFiltersButton.addEventListener('click', () => {
      this.applyFilters(createActivityFeedFilters());
      if (!this.filtersButton.hidden) {
        this.projectFilterSelect.focus({ preventScroll: true });
      }
    });

    const header = h(
      'header',
      { className: 'aq-feed-header tl-head' },
      h(
        'div',
        { className: 'aq-feed-heading' },
        h('h2', { className: 'aq-feed-title' }, 'Activity'),
        this.runningCountEl
      )
    );
    const tools = h(
      'div',
      { className: 'aq-feed-tools tl-tools' },
      this.scopeEl,
      this.allActivityButton,
      this.filtersButton
    );

    this.footerEl = h(
      'footer',
      { className: 'aq-feed-footer', hidden: true },
      h(
        'div',
        { className: 'aq-feed-footer-actions' },
        this.footerNoteEl,
        this.loadMoreButton
      )
    );

    this.element = h(
      'section',
      {
        className: 'aq-activity-feed',
        'aria-label': 'Activity request feed',
        'data-activity-feed': 'true',
      },
      header,
      tools,
      this.filterSummaryEl,
      this.filterPanelEl,
      this.listEl,
      this.emptyEl,
      this.footerEl,
      this.noticesEl
    );

    this.scopeEl.textContent = 'All recorded activity · Available sources';
    this.emptyEl.hidden = false;
    this.emptyEl.textContent = 'No activity snapshot yet.';
  }

  update(
    snapshot: ObservationSnapshot | null,
    options: ActivityFeedUpdateOptions = {}
  ): void {
    if (this.disposed) {
      return;
    }

    const scroller = this.element.closest('.aq-main') as HTMLElement | null;
    const scrollTop = scroller?.scrollTop ?? null;
    const focusKey = this.captureFocusKey();

    // Explicit null is a source/scope reset. Ordinary refreshes already receive
    // their retained non-null snapshot from the app's observation view state.
    this.snapshot = snapshot;
    this.loading = options.loading === true;
    this.error = options.error ?? null;
    if (options.scope !== undefined || snapshot === null) {
      this.scope = options.scope ?? null;
    }
    if (snapshot === null) {
      this.filters = createActivityFeedFilters();
      this.filtersOpen = false;
      this.clearFilterOptionCache();
    }

    if (this.snapshot) {
      const projection = projectActivityFeed(this.snapshot, {
        scope: this.scope,
        filters: this.filters,
      });
      this.filters = projection.filters;
      this.renderProjection(projection);
    } else {
      this.renderWithoutSnapshot();
    }

    this.restoreFocusAndScroll(focusKey, scroller, scrollTop);
  }

  /**
   * Lightweight clock tick: update only the existing relative context time and
   * elapsed-duration text nodes. Rows, detail state, focus, and scroll stay put.
   */
  refreshTimeLabels(): void {
    if (this.disposed) {
      return;
    }
    const nowMs = Date.now();
    for (const entry of this.rows.values()) {
      if (entry.contextTimeEl && entry.contextAtMs !== null) {
        const next = formatRelativeTime(entry.contextAtMs, nowMs);
        if (entry.contextTimeEl.textContent !== next) {
          entry.contextTimeEl.textContent = next;
        }
      }
      if (entry.durationEl && entry.elapsedStartedAtMs !== null) {
        const next = formatDurationMs(
          Math.max(0, nowMs - entry.elapsedStartedAtMs)
        );
        if (entry.durationEl.textContent !== next) {
          entry.durationEl.textContent = next;
        }
      }
    }
  }

  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.rows.clear();
    this.expanded.clear();
    this.snapshot = null;
    this.element.replaceChildren();
  }

  private applyFilters(filters: ActivityFeedFilters): void {
    if (!this.snapshot) {
      return;
    }
    const scroller = this.element.closest('.aq-main') as HTMLElement | null;
    const scrollTop = scroller?.scrollTop ?? null;
    const projection = projectActivityFeed(this.snapshot, {
      scope: this.scope,
      filters,
    });
    this.filters = projection.filters;
    this.renderProjection(projection);
    if (scroller && scrollTop !== null) {
      scroller.scrollTop = scrollTop;
    }
  }

  private syncFilterPanel(): void {
    const open = this.filtersOpen && this.filtersAvailable;
    this.filterPanelEl.hidden = !open;
    this.filtersButton.setAttribute('aria-expanded', open ? 'true' : 'false');
    this.filtersButton.title = open
      ? 'Hide activity filters'
      : 'Show activity filters';
    this.filterSummaryEl.hidden =
      !this.filtersAvailable || this.activeFilterCount === 0 || open;
  }

  private renderFilters(projection: ActivityFeedProjection): void {
    this.filtersAvailable =
      projection.scopeRowCount > 0 || projection.activeFilterCount > 0;
    this.activeFilterCount = projection.activeFilterCount;
    this.filtersButton.hidden = !this.filtersAvailable;
    this.filtersButton.textContent =
      projection.activeFilterCount > 0
        ? `Filters (${projection.activeFilterCount})`
        : 'Filters';
    this.filtersButton.setAttribute(
      'aria-label',
      projection.activeFilterCount > 0
        ? `Filters, ${projection.activeFilterCount} active`
        : 'Filters'
    );
    this.filtersButton.dataset.active =
      projection.activeFilterCount > 0 ? 'true' : 'false';
    this.syncFilterPanel();

    const coverage = formatActivityFilterCoverage(
      projection.scopeRowCount,
      projection.visibleRowCount,
      projection.activeFilterCount
    );
    this.filterStatusEl.textContent = coverage;
    this.filterSummaryEl.textContent = coverage;
    this.clearFiltersButton.disabled = projection.activeFilterCount === 0;

    this.renderFilterSelect(
      'projects',
      this.projectFilterSelect,
      'All projects',
      projection.filterOptions.projects,
      this.filters.projectKey,
      projection.scopeRowCount
    );
    this.renderFilterSelect(
      'chats',
      this.chatFilterSelect,
      'All chats',
      projection.filterOptions.chats,
      this.filters.chatKey,
      projection.scopeRowCount
    );
    this.renderFilterSelect(
      'states',
      this.stateFilterSelect,
      'All states',
      projection.filterOptions.states,
      this.filters.state,
      projection.scopeRowCount
    );
  }

  private renderFilterSelect(
    dimension: ActivityFilterDimension,
    select: HTMLSelectElement,
    allLabel: string,
    options: readonly ActivityFilterOption[],
    selectedKey: string | null,
    scopeRowCount: number
  ): void {
    const cache = this.filterOptionCache[dimension];
    for (const option of options) {
      cache.set(option.key, option);
    }
    const merged = [...options];
    if (selectedKey && !merged.some((option) => option.key === selectedKey)) {
      const cached = cache.get(selectedKey);
      merged.unshift({
        key: selectedKey,
        label: cached?.label ?? 'Selected filter',
        detail:
          cached?.detail ??
          'The selected filter is not present in the loaded activity.',
        count: 0,
      });
    }
    const signature = JSON.stringify([merged, selectedKey, scopeRowCount]);
    if (this.filterOptionSignatures[dimension] === signature) {
      if (select.value !== (selectedKey ?? '')) {
        select.value = selectedKey ?? '';
      }
      return;
    }

    const optionElements = merged.map((option) =>
      h(
        'option',
        { value: option.key, title: filterOptionTitle(option) },
        `${option.label} (${option.count})`
      )
    );
    select.replaceChildren(
      h('option', { value: '' }, `${allLabel} (${scopeRowCount})`),
      ...optionElements
    );
    select.value = selectedKey ?? '';
    this.filterOptionSignatures[dimension] = signature;
  }

  private clearFilterOptionCache(): void {
    for (const dimension of [
      'projects',
      'chats',
      'states',
    ] as ActivityFilterDimension[]) {
      this.filterOptionCache[dimension].clear();
      this.filterOptionSignatures[dimension] = '';
    }
  }

  private renderProjection(projection: ActivityFeedProjection): void {
    this.element.setAttribute('aria-busy', this.loading ? 'true' : 'false');
    this.renderHeader(projection);
    this.renderFilters(projection);
    this.renderNotices(projection);
    this.reconcileRows(projection);
    this.renderEmpty(projection);
    this.renderFooter(projection);
  }

  private renderWithoutSnapshot(): void {
    this.element.setAttribute('aria-busy', this.loading ? 'true' : 'false');

    if (this.scope) {
      this.scopeEl.textContent = `Job ${this.scope.job_id}`;
      this.scopeEl.title = `Source ${this.scope.source_id} · Job ${this.scope.job_id}`;
      this.scopeEl.setAttribute(
        'aria-label',
        `Scoped to source ${this.scope.source_id} · job ${this.scope.job_id}`
      );
      this.allActivityButton.hidden = false;
    } else {
      this.scopeEl.textContent = 'All recorded activity · Available sources';
      this.scopeEl.title = 'All recorded activity across available sources';
      this.scopeEl.setAttribute(
        'aria-label',
        'All recorded activity across available sources'
      );
      this.allActivityButton.hidden = true;
    }
    this.runningCountEl.hidden = true;
    this.runningCountEl.textContent = '';
    this.filtersAvailable = false;
    this.activeFilterCount = 0;
    this.filtersButton.hidden = true;
    this.filtersButton.setAttribute('aria-expanded', 'false');
    this.filterPanelEl.hidden = true;
    this.filterSummaryEl.hidden = true;
    this.filterSummaryEl.textContent = '';
    this.filterStatusEl.textContent = '';

    for (const entry of this.rows.values()) {
      entry.li.remove();
    }
    this.rows.clear();

    this.noticesEl.replaceChildren(
      ...(this.error
        ? [
            h(
              'div',
              {
                className: 'aq-feed-notice aq-feed-notice--danger',
                role: 'alert',
              },
              this.error
            ),
          ]
        : [])
    );

    this.emptyEl.hidden = false;
    this.emptyEl.textContent = this.error
      ? 'Activity is unavailable.'
      : this.loading
        ? 'Loading recorded activity…'
        : 'No activity snapshot yet.';

    this.footerEl.hidden = true;
    this.footerNoteEl.hidden = true;
    this.footerNoteEl.textContent = '';
    this.loadMoreButton.hidden = true;
    this.loadMoreButton.disabled = true;
  }

  private renderHeader(projection: ActivityFeedProjection): void {
    this.scopeEl.textContent = formatActivityScopeLabel(projection.scope);
    this.scopeEl.title = projection.scope.scoped
      ? projection.scope.ids
      : 'All recorded activity across available sources';
    this.scopeEl.setAttribute(
      'aria-label',
      projection.scope.scoped
        ? `${formatActivityScopeLabel(projection.scope)}. ${projection.scope.ids}`
        : 'All recorded activity across available sources'
    );
    this.allActivityButton.hidden = !projection.scope.scoped;
    const running = countRunningRequests(projection.rows);
    this.runningCountEl.hidden = projection.scopeRunningCount === 0;
    const runningText =
      running === projection.scopeRunningCount
        ? formatRunningCountLabel(running)
        : formatActivityRunningCountLabel(
            running,
            projection.scopeRunningCount
          );
    if (this.runningCountEl.textContent !== runningText) {
      this.runningCountEl.textContent = runningText;
    }
  }

  private renderNotices(projection: ActivityFeedProjection): void {
    const notices: HTMLElement[] = [];

    if (this.error) {
      notices.push(
        h(
          'div',
          {
            className: 'aq-feed-notice aq-feed-notice--danger',
            role: 'alert',
          },
          this.error,
          projection.rows.length > 0
            ? ' Last available snapshot; status may be stale until a refresh succeeds.'
            : ''
        )
      );
    }

    if (projection.unavailableSources.length > 0) {
      notices.push(
        h(
          'div',
          {
            className: 'aq-feed-notice aq-feed-notice--warn',
            role: 'status',
          },
          h('p', { className: 'aq-feed-notice-title' }, 'Unavailable sources'),
          h(
            'ul',
            { className: 'aq-feed-notice-list' },
            ...projection.unavailableSources.map((item) =>
              h(
                'li',
                {},
                `${item.label} unavailable: ${item.message}`
              )
            )
          )
        )
      );
    }

    if (projection.warnings.length > 0) {
      notices.push(
        h(
          'div',
          {
            className: 'aq-feed-notice aq-feed-notice--warn',
            role: 'status',
          },
          h('p', { className: 'aq-feed-notice-title' }, 'Snapshot warnings'),
          h(
            'ul',
            { className: 'aq-feed-notice-list' },
            ...projection.warnings.map((warning) => h('li', {}, warning))
          )
        )
      );
    }

    if (projection.jobsHasMore) {
      notices.push(
        h(
          'div',
          {
            className: 'aq-feed-notice aq-feed-notice--info',
            role: 'status',
          },
          'More jobs exist beyond this snapshot. Some job details may be unavailable.'
        )
      );
    }

    if (projection.scope.scoped && projection.scope.jobUnavailable) {
      notices.push(
        h(
          'div',
          {
            className: 'aq-feed-notice aq-feed-notice--warn',
            role: 'status',
          },
          'Job details are unavailable for the selected scope.'
        )
      );
    }

    this.noticesEl.replaceChildren(...notices);
  }

  private reconcileRows(projection: ActivityFeedProjection): void {
    const desired = new Set<string>();

    for (const view of projection.rows) {
      desired.add(view.key);
      if (!this.rows.has(view.key)) {
        this.rows.set(view.key, this.createRow(view));
      }
    }

    for (const [key, entry] of Array.from(this.rows.entries())) {
      if (!desired.has(key)) {
        entry.li.remove();
        this.rows.delete(key);
      }
    }

    projection.rows.forEach((view, index) => {
      const entry = this.rows.get(view.key);
      if (!entry) {
        return;
      }
      const current = this.listEl.children[index] ?? null;
      if (current !== entry.li) {
        this.listEl.insertBefore(entry.li, current);
      }
    });

    for (const view of projection.rows) {
      const entry = this.rows.get(view.key);
      if (entry) {
        this.updateRow(entry, view);
      }
    }
  }

  private createRow(view: ActivityRequestView): RenderedRow {
    const li = h('li', {
      className: 'aq-feed-item tl-row',
      'data-feed-key': view.domKey,
    });
    const details = h('details', {
      className: 'aq-feed-row',
      'data-feed-key': view.domKey,
    });
    const summary = h('summary', { className: 'aq-feed-summary' });
    const detail = h('div', {
      className: 'aq-feed-detail tl-expand tl-detail',
    });

    details.addEventListener('toggle', () => {
      if (this.disposed) {
        return;
      }
      if (details.open) {
        this.expanded.add(view.key);
      } else {
        this.expanded.delete(view.key);
      }
    });
    details.addEventListener('click', (event) => {
      if (this.disposed) {
        return;
      }
      const target = event.target;
      const currentSummary = details.querySelector('summary');
      if (
        !(target instanceof Node) ||
        !currentSummary ||
        !currentSummary.contains(target)
      ) {
        return;
      }
      if (details.open) {
        this.expanded.delete(view.key);
      } else {
        this.expanded.add(view.key);
      }
    });

    details.append(summary, detail);
    li.appendChild(details);

    return {
      key: view.key,
      li,
      details,
      summary,
      signature: '',
      contextTimeEl: null,
      durationEl: null,
      contextAtMs: null,
      elapsedStartedAtMs: null,
      lastView: null,
    };
  }

  private updateRow(entry: RenderedRow, view: ActivityRequestView): void {
    const parentKey = view.parentRequestId
      ? requestIdentityKey(view.request.source_id, view.parentRequestId)
      : null;
    const parentPresent = parentKey !== null && this.rows.has(parentKey);
    const signature = activityRowSignature(
      view,
      parentPresent,
      this.error !== null
    );

    if (entry.signature === signature) {
      const open = this.expanded.has(view.key);
      if (entry.details.open !== open) {
        entry.details.open = open;
      }
      this.updateElapsedDurationText(entry, entry.lastView, view);
      entry.lastView = view;
      const duration = formatCompactDuration(
        view.duration,
        view.durationDetail
      );
      if (entry.durationEl) {
        if (entry.durationEl.textContent !== duration.value) {
          entry.durationEl.textContent = duration.value;
        }
        if (entry.durationEl.title !== duration.detail) {
          entry.durationEl.title = duration.detail;
          entry.durationEl.setAttribute('aria-label', duration.detail);
        }
      }
      entry.elapsedStartedAtMs =
        view.duration.kind === 'elapsed'
          ? recordedTimestampMs(view.request.started_at_ms)
          : null;
      return;
    }

    entry.li.dataset.state = view.state;
    entry.li.dataset.tone = view.stateTone;
    const technicalOpen =
      entry.details.querySelector<HTMLDetailsElement>('.aq-feed-technical')?.open ?? false;
    const summary = this.buildSummary(view);
    const detail = this.buildDetail(view);
    const technical = detail.querySelector<HTMLDetailsElement>('.aq-feed-technical');
    if (technical) technical.open = technicalOpen;
    entry.details.open = this.expanded.has(view.key);
    entry.details.replaceChildren(summary, detail);
    entry.summary = summary;
    entry.signature = signature;
    entry.contextTimeEl = summary.querySelector<HTMLElement>(
      '[data-feed-context-time]'
    );
    entry.durationEl = summary.querySelector<HTMLElement>(
      '[data-feed-duration]'
    );
    entry.contextAtMs = recordedTimestampMs(view.request.updated_at_ms);
    entry.elapsedStartedAtMs =
      view.duration.kind === 'elapsed'
        ? recordedTimestampMs(view.request.started_at_ms)
        : null;
    entry.lastView = view;
  }

  private updateElapsedDurationText(
    entry: RenderedRow,
    previous: ActivityRequestView | null,
    next: ActivityRequestView
  ): void {
    if (!previous) {
      return;
    }
    const before = elapsedDurationTexts(previous);
    const after = elapsedDurationTexts(next);
    if (before.length !== after.length) {
      return;
    }
    const replacements = new Map<string, string | null>();
    for (let index = 0; index < before.length; index += 1) {
      const oldText = before[index]!;
      const newText = after[index]!;
      if (oldText === newText) {
        continue;
      }
      const existing = replacements.get(oldText);
      if (existing === undefined) {
        replacements.set(oldText, newText);
      } else if (existing !== newText) {
        // Ambiguous repeated elapsed values are safer to leave until a full
        // meaningful row refresh than to rewrite the wrong evidence line.
        replacements.set(oldText, null);
      }
    }
    if (replacements.size === 0) {
      return;
    }
    for (const text of entry.details.querySelectorAll<HTMLElement>('p')) {
      const replacement = replacements.get(text.textContent ?? '');
      if (typeof replacement === 'string' && text.textContent !== replacement) {
        text.textContent = replacement;
      }
    }
  }

  private buildSummary(view: ActivityRequestView): HTMLElement {
    const status = this.buildStatusBadge(
      view.stateLabel,
      view.stateTone,
      view.state,
      view.showSpinner && this.error === null
    );
    const duration = formatCompactDuration(view.duration, view.durationDetail);
    const usage = formatCompactUsage(view.usage);
    const executor = projectRequestExecutorDisplay(view);
    const requester = view.requesterLine;
    const requesterRole = view.request.actor.role?.trim();
    const requesterValue = view.request.actor.label?.trim() && requesterRole
      ? `${requester.value} · ${formatExecutorRoleLabel(requesterRole)}`
      : requester.value;
    const jobText = view.jobLinked ? view.jobLabel : 'No linked job';
    const brand = resolveExecutorBrand(view.request.executor);
    const contextAtMs = recordedTimestampMs(view.request.updated_at_ms);
    const contextTime = formatRelativeTime(contextAtMs);

    return h(
      'summary',
      {
        className: 'aq-feed-summary',
        'data-focus-key': `feed:${view.domKey}:summary`,
      },
      h(
        'span',
        { className: 'aq-feed-context tl-context' },
        h(
          'span',
          { className: 'aq-feed-context-lead' },
          h(
            'span',
            { className: 'aq-feed-project', title: view.projectDetail },
            view.projectLabel
          ),
          h(
            'span',
            {
              className: 'aq-feed-chat',
              title: view.chatDetail,
              'aria-label': view.chatDetail,
            },
            view.chatLabel
          )
        ),
        h(
          'span',
          {
            className: 'aq-feed-context-time',
            'data-feed-context-time': 'true',
            title:
              contextAtMs === null
                ? 'Recorded time not reported'
                : formatFeedTimestamp(contextAtMs),
          },
          contextTime
        )
      ),
      h(
        'span',
        { className: 'aq-feed-main' },
        h(
          'span',
          { className: 'aq-feed-icon', 'aria-hidden': 'true' },
          createBrandIconElement(brand)
        ),
        h(
          'span',
          { className: 'aq-feed-row-title tl-title', title: view.title },
          view.title
        )
      ),
      h(
        'span',
        {
          className: 'aq-feed-sub tl-sub aq-feed-job-line',
          title: `Job: ${jobText}`,
        },
        `Job: ${jobText}`
      ),
      h(
        'span',
        {
          className: 'aq-feed-sub tl-sub tl-requester aq-feed-requester',
          title: requester.detail,
          'aria-label': `Requested by ${requester.value}`,
        },
        h('span', { className: 'aq-feed-requested-label' }, 'Requested by'),
        h('span', { className: 'aq-feed-requested-value' }, requesterValue)
      ),
      h(
        'span',
        { className: 'aq-feed-role-line', title: executor.detail },
        h(
          'span',
          {
            className: 'aq-feed-executor-value',
            title: executor.detail,
            'aria-label': executor.detail,
          },
          executor.model ? `${executor.role} · ${executor.model}` : executor.role
        ),
        status
      ),
      h(
        'span',
        { className: 'aq-feed-metrics' },
        h(
          'span',
          {
            className: 'aq-feed-measure',
            'data-feed-duration': 'true',
            title: duration.detail,
            'aria-label': duration.detail,
          },
          duration.value
        ),
        h(
          'span',
          {
            className: 'aq-feed-measure aq-feed-usage',
            title: usage.detail,
            'aria-label': usage.detail,
          },
          usage.value
        )
      )
    );
  }

  private buildDetail(view: ActivityRequestView): HTMLElement {
    const detail = h(
      'div',
      { className: 'aq-feed-detail tl-expand tl-detail' },
      h(
        'div',
        { className: 'aq-feed-detail-body' },
        this.buildTimeline(view)
      )
    );
    const actions: HTMLElement[] = [];

    if (view.jobLinked && view.request.job_id && this.onViewJob) {
      const jobId = view.request.job_id;
      const viewJobButton = h(
        'button',
        {
          type: 'button',
          className: 'aq-btn aq-feed-view-job',
          'data-focus-key': `feed:${view.domKey}:view-job`,
          'aria-label': `View job ${jobId}`,
        },
        'View job'
      );
      viewJobButton.addEventListener('click', (event) => {
        event.stopPropagation();
        this.onViewJob?.({
          source_id: view.request.source_id,
          job_id: jobId,
        });
      });
      actions.push(viewJobButton);
    }

    if (view.parentRequestId) {
      const parentKey = requestIdentityKey(
        view.request.source_id,
        view.parentRequestId
      );
      const parentPresent = this.rows.has(parentKey);
      const parentButton = h(
        'button',
        {
          type: 'button',
          className: 'aq-feed-link',
          'data-focus-key': `feed:${view.domKey}:parent`,
          'aria-label': `Open parent request ${view.parentRequestId}`,
        },
        `Parent ${view.parentRequestId}`
      );
      parentButton.addEventListener('click', (event) => {
        event.stopPropagation();
        this.openRequest(parentKey);
      });
      if (!parentPresent) {
        parentButton.disabled = true;
        parentButton.title = 'Parent request is not included in this snapshot.';
      }
      actions.push(parentButton);
    }

    if (actions.length > 0) {
      detail.appendChild(
        h('div', { className: 'aq-feed-detail-actions' }, ...actions)
      );
    }

    detail.appendChild(
      h(
        'details',
        { className: 'aq-feed-technical' },
        h('summary', { 'data-focus-key': `feed:${view.domKey}:technical` }, 'Recorded evidence'),
        ...view.timeline.map((event) =>
          h(
            'div',
            { className: 'aq-feed-technical-event' },
            h('p', { className: 'aq-feed-technical-label' }, event.label),
            h('p', {}, event.detail),
            ...event.lines.map((line) => h('p', {}, line)),
            event.note ? h('p', {}, event.note) : null
          )
        )
      )
    );

    return detail;
  }

  private buildTimeline(view: ActivityRequestView): HTMLElement {
    return h(
      'ol',
      {
        className: 'aq-feed-timeline',
        'aria-label': 'Recorded request timeline',
      },
      ...projectVisibleTimeline(view).map((entry) => this.buildTimelineEvent(entry))
    );
  }

  private buildTimelineEvent({ event, display }: ActivityVisibleTimelineEvent): HTMLElement {
    return h(
      'li',
      {
        className: 'aq-feed-event tl-event',
        'data-kind': event.kind,
        'data-tone': event.tone,
        'data-stage': event.stage,
        title:
          event.atMs === null
            ? 'Recorded time not reported'
            : `Recorded ${event.atValue}`,
      },
      h('span', { className: 'aq-feed-event-label' }, display.label),
      h('p', { className: 'aq-feed-event-copy' }, display.detail),
      ...display.lines.map((line) =>
        h('p', { className: 'aq-feed-event-line' }, line)
      )
    );
  }

  private buildStatusBadge(
    label: string,
    tone: LifecycleTone,
    state: ObservationLifecycleState,
    showSpinner: boolean
  ): HTMLElement {
    const children: Array<Node | string> = [];
    if (showSpinner) {
      children.push(
        h(
          'span',
          { className: 'aq-feed-spinner', 'aria-hidden': 'true' },
          createUiIcon('refresh-cw')
        )
      );
    }
    children.push(h('span', { className: 'aq-feed-status-text' }, label));

    return h(
      'span',
      {
        className: `aq-feed-status aq-feed-status--${tone}`,
        'data-state': state,
        'aria-label': `Lifecycle: ${label}`,
      },
      ...children
    );
  }

  private renderEmpty(projection: ActivityFeedProjection): void {
    if (projection.rows.length > 0) {
      this.emptyEl.hidden = true;
      this.emptyEl.textContent = '';
      return;
    }

    this.emptyEl.hidden = false;
    if (this.loading) {
      this.emptyEl.textContent = 'Loading recorded activity…';
    } else if (
      projection.activeFilterCount > 0 &&
      projection.scopeRowCount > 0
    ) {
      this.emptyEl.textContent = `No loaded activity matches the selected filters. ${formatActivityFilterCoverage(
        projection.scopeRowCount,
        projection.visibleRowCount,
        projection.activeFilterCount
      )} Use Clear filters to show all loaded activity.`;
    } else if (projection.scope.scoped) {
      this.emptyEl.textContent = 'No recorded requests match this job scope.';
    } else if (projection.unavailableSources.length > 0) {
      this.emptyEl.textContent =
        'No recorded activity is available from the current sources.';
    } else {
      this.emptyEl.textContent = 'No recorded activity yet.';
    }
  }

  private renderFooter(projection: ActivityFeedProjection): void {
    if (projection.hasMore && this.onLoadMore) {
      this.footerEl.hidden = false;
      this.loadMoreButton.hidden = false;
      this.loadMoreButton.disabled = this.loading && projection.rows.length === 0;
      this.loadMoreButton.textContent = 'Load more';
      this.footerNoteEl.hidden = true;
      this.footerNoteEl.textContent = '';
      return;
    }

    this.loadMoreButton.hidden = true;
    this.loadMoreButton.disabled = true;
    if (projection.hasMore) {
      this.footerEl.hidden = false;
      this.footerNoteEl.hidden = false;
      this.footerNoteEl.textContent = 'More requests are available.';
    } else {
      this.footerEl.hidden = true;
      this.footerNoteEl.hidden = true;
      this.footerNoteEl.textContent = '';
    }
  }

  private openRequest(key: string): void {
    const entry = this.rows.get(key);
    if (!entry) {
      return;
    }
    this.expanded.add(key);
    entry.details.open = true;
    entry.summary.focus({ preventScroll: true });
    entry.li.scrollIntoView({ block: 'nearest' });
  }

  private captureFocusKey(): string | null {
    if (typeof document === 'undefined') {
      return null;
    }
    const active = document.activeElement;
    if (!(active instanceof HTMLElement) || !this.element.contains(active)) {
      return null;
    }
    return active.dataset.focusKey ?? null;
  }

  private restoreFocusAndScroll(
    focusKey: string | null,
    scroller: HTMLElement | null,
    scrollTop: number | null
  ): void {
    if (scroller && scrollTop !== null) {
      scroller.scrollTop = scrollTop;
    }
    if (!focusKey) {
      return;
    }
    const target = Array.from(
      this.element.querySelectorAll<HTMLElement>('[data-focus-key]')
    ).find((candidate) => candidate.dataset.focusKey === focusKey);
    if (target && !target.hasAttribute('disabled') && !target.hidden) {
      target.focus({ preventScroll: true });
    }
  }
}
