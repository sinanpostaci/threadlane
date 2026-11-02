import { createBrandIconElement, createUiIcon } from '../icons.ts';
import { h } from '../ui/dom.ts';
import type { ObservationSnapshot } from '../observation/types.ts';
import {
  countRunningRequests,
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
  type ActivityFeedProjection,
  type ActivityFeedScope,
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
}

function recordedTimestampMs(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value > 0
    ? value
    : null;
}

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
  private readonly noticesEl: HTMLElement;
  private readonly listEl: HTMLUListElement;
  private readonly emptyEl: HTMLElement;
  private readonly footerEl: HTMLElement;
  private readonly footerNoteEl: HTMLElement;
  private readonly loadMoreButton: HTMLButtonElement;
  private readonly rows = new Map<string, RenderedRow>();
  private readonly expanded = new Set<string>();
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
      this.allActivityButton
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

    if (this.snapshot) {
      this.renderProjection(
        projectActivityFeed(this.snapshot, { scope: this.scope })
      );
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

  private renderProjection(projection: ActivityFeedProjection): void {
    this.element.setAttribute('aria-busy', this.loading ? 'true' : 'false');
    this.renderHeader(projection);
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
    this.runningCountEl.hidden = running === 0;
    const runningText = formatRunningCountLabel(running);
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
          projection.rows.length > 0 ? ' Showing available rows.' : ''
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
    };
  }

  private updateRow(entry: RenderedRow, view: ActivityRequestView): void {
    const parentKey = view.parentRequestId
      ? requestIdentityKey(view.request.source_id, view.parentRequestId)
      : null;
    const parentPresent = parentKey !== null && this.rows.has(parentKey);
    const signature = JSON.stringify([view, parentPresent]);

    if (entry.signature === signature) {
      const open = this.expanded.has(view.key);
      if (entry.details.open !== open) {
        entry.details.open = open;
      }
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
  }

  private buildSummary(view: ActivityRequestView): HTMLElement {
    const status = this.buildStatusBadge(
      view.stateLabel,
      view.stateTone,
      view.state,
      view.showSpinner
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
          { className: 'aq-feed-project', title: view.projectLabel },
          view.projectLabel
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
