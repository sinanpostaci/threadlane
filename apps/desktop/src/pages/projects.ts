import { createBrandIconElement, createUiIcon, type UiIconName } from '../icons.ts';
import type { ObservationSnapshot } from '../observation/types.ts';
import type { BrandKey } from '../types.ts';
import { h } from '../ui/dom.ts';
import {
  formatRelativeTime,
  type ActivityFeedScope,
} from './activity-feed-model.ts';
import {
  projectProjects,
  type ProjectAttentionItemView,
  type ProjectCallView,
  type ProjectDiagnosticRowView,
  type ProjectEventView,
  type ProjectGroupView,
  type ProjectJobView,
  type ProjectSessionView,
  type ProjectsProjection,
  type ProjectsWindowHours,
} from './projects-model.ts';
import {
  readProjectsArchiveCutoff,
  writeProjectsArchiveCutoff,
  type ProjectsArchiveStorage,
} from './projects-archive.ts';

import { projectTaskStatus } from './projects-task-status.ts';
import './projects.css';

const MAX_RECENT_EVENTS = 6;

function resolveBrowserArchiveStorage(): ProjectsArchiveStorage | null {
  try {
    if (typeof globalThis !== 'undefined' && 'localStorage' in globalThis) {
      return globalThis.localStorage ?? null;
    }
  } catch {
    return null;
  }
  return null;
}

function formatRecordedAge(atMs: number | null, nowMs: number): string {
  if (atMs === null) {
    return 'Time not reported';
  }
  return `Recorded ${formatRelativeTime(atMs, nowMs)}`;
}

function summarizeAttentionBreakdown(
  items: readonly ProjectAttentionItemView[]
): string {
  const counts = {
    on_hold: 0,
    plan_approval: 0,
    awaiting_review: 0,
    recorded_wait: 0,
    status_check: 0,
  };
  for (const item of items) {
    counts[item.kind] += 1;
  }
  const parts: string[] = [];
  if (counts.on_hold > 0) {
    parts.push(`${counts.on_hold} on hold`);
  }
  if (counts.plan_approval > 0) {
    parts.push(
      `${counts.plan_approval} ${counts.plan_approval === 1 ? 'plan' : 'plans'} awaiting approval`
    );
  }
  if (counts.awaiting_review > 0) {
    parts.push(`${counts.awaiting_review} awaiting review`);
  }
  if (counts.recorded_wait > 0) {
    parts.push(
      `${counts.recorded_wait} recorded ${counts.recorded_wait === 1 ? 'wait' : 'waits'}`
    );
  }
  if (counts.status_check > 0) {
    parts.push(
      `${counts.status_check} ${counts.status_check === 1 ? 'status' : 'statuses'} to check`
    );
  }
  return parts.join(' · ');
}

export interface ProjectsPageOptions {
  onViewJob?: (scope: { source_id: string; job_id: string }) => void;
  onLoadMore?: () => void;
  onClearScope?: () => void;
  storage?: ProjectsArchiveStorage | null;
}

export interface ProjectsUpdateOptions {
  loading?: boolean;
  error?: string | null;
  scope?: ActivityFeedScope | null;
}

type SessionIndicatorKind = 'working' | 'waiting' | 'danger' | 'completed' | 'neutral';

interface JobDomEntry {
  key: string;
  jobView: ProjectJobView;
  element: HTMLDetailsElement;
  phaseStateEl: HTMLSpanElement;
  planLabelEl: HTMLSpanElement;
  phaseRightEl: HTMLSpanElement;
  titleEl: HTMLElement;
  fullTitleEl: HTMLParagraphElement;
  recordedAgeEl: HTMLSpanElement;
  summaryEl: HTMLParagraphElement;
  tasksListEl: HTMLUListElement;
  approvalNoteEl: HTMLParagraphElement;
  detailMetaEl: HTMLSpanElement;
}

const userJobOpenStateByInstance = new WeakMap<object, Map<string, boolean>>();

function getUserJobOpenState(instance: object): Map<string, boolean> {
  let state = userJobOpenStateByInstance.get(instance);
  if (!state) {
    state = new Map<string, boolean>();
    userJobOpenStateByInstance.set(instance, state);
  }
  return state;
}

interface CallDomEntry {
  key: string;
  call: ProjectCallView;
  brand: BrandKey;
  element: HTMLDivElement;
  iconEl: HTMLElement;
  roleTextNode: Text;
  purposeEl: HTMLSpanElement;
  modelEl: HTMLSpanElement;
  metricFirstRowEl: HTMLSpanElement;
  spinnerEl: HTMLSpanElement;
  durationTextEl: HTMLSpanElement;
  usageEl: HTMLSpanElement;
  detailMetaEl: HTMLSpanElement;
}

interface EventDomEntry {
  key: string;
  event: ProjectEventView;
  element: HTMLLIElement;
  labelEl: HTMLSpanElement;
  timeEl: HTMLElement;
  detailMetaEl: HTMLSpanElement;
}

interface SessionDomEntry {
  key: string;
  session: ProjectSessionView;
  details: HTMLDetailsElement;
  summary: HTMLElement;
  nameEl: HTMLSpanElement;
  stateEl: HTMLSpanElement;
  indicatorKind: SessionIndicatorKind | null;
  stateTextNode: Text;
  ageEl: HTMLSpanElement;
  detailMetaEl: HTMLSpanElement;
  bodyEl: HTMLDivElement;
  pendingSectionEl: HTMLDivElement;
  pendingHeadingEl: HTMLDivElement;
  pendingSummaryEl: HTMLParagraphElement;
  pendingListEl: HTMLUListElement;
  jobsContainer: HTMLDivElement;
  olderJobsDetails: HTMLDetailsElement;
  olderJobsSummaryTextEl: HTMLSpanElement;
  olderJobsListEl: HTMLDivElement;
  jobEntries: Map<string, JobDomEntry>;
  callsContainer: HTMLDivElement;
  callEntries: Map<string, CallDomEntry>;
  recentDetails: HTMLDetailsElement;
  eventsList: HTMLOListElement;
  eventEntries: Map<string, EventDomEntry>;
  quietNoteEl: HTMLParagraphElement;
}

interface ProjectDomEntry {
  key: string;
  project: ProjectGroupView;
  details: HTMLDetailsElement;
  summary: HTMLElement;
  nameEl: HTMLSpanElement;
  statusEl: HTMLSpanElement;
  sessionsContainer: HTMLDivElement;
  emptyEl: HTMLParagraphElement;
  sessionEntries: Map<string, SessionDomEntry>;
}

interface DiagnosticDomEntry {
  key: string;
  diag: ProjectDiagnosticRowView;
  element: HTMLLIElement;
  dotEl: HTMLSpanElement;
  titleEl: HTMLSpanElement;
  metaEl: HTMLParagraphElement;
  impactEl: HTMLParagraphElement;
  nextStepEl: HTMLParagraphElement;
  scopeNoteEl: HTMLParagraphElement;
  technicalDetailsEl: HTMLDetailsElement;
  technicalSummaryEl: HTMLElement;
  technicalTextEl: HTMLParagraphElement;
}

function hasActiveScope(scope: ActivityFeedScope | null | undefined): boolean {
  return Boolean(scope && (scope.source_id || scope.job_id));
}

const pendingListSignatures = new WeakMap<object, string>();

type SessionStatusTone =
  | 'working'
  | 'ok'
  | 'waiting'
  | 'danger'
  | 'completed'
  | 'review'
  | 'queued'
  | 'unknown'
  | 'neutral';

function jobPhaseTone(jobView: ProjectJobView): SessionStatusTone {
  const state = jobView.job.state.trim().toLowerCase();
  const execution = (jobView.job.execution_status ?? '').trim().toLowerCase();
  if (
    state === 'failed' ||
    state === 'timed_out' ||
    execution === 'failed' ||
    execution === 'timed_out'
  ) {
    return 'danger';
  }
  if (
    state === 'completed' ||
    state === 'accepted' ||
    state === 'integrated'
  ) {
    return 'ok';
  }
  if (
    jobView.attentionKind === 'on_hold' ||
    jobView.attentionKind === 'plan_approval' ||
    jobView.attentionKind === 'recorded_wait'
  ) {
    return 'waiting';
  }
  if (jobView.attentionKind === 'awaiting_review') {
    return 'review';
  }
  if (jobView.attentionKind === 'status_check') {
    return /planning/i.test(jobView.stateLabel) ? 'review' : 'unknown';
  }
  if (
    state === 'needs_user' ||
    state === 'on_hold' ||
    execution === 'held'
  ) {
    return 'waiting';
  }
  if (
    state === 'queued' ||
    state === 'not_started' ||
    state === 'pending_dependencies'
  ) {
    return 'queued';
  }
  if (state === 'planning') {
    return 'review';
  }
  if (state === 'running' || state === 'reviewing' || state === 'claimed') {
    return 'working';
  }
  return 'neutral';
}

function sessionStatusTone(session: ProjectSessionView): SessionStatusTone {
  if (session.runningCount > 0 || session.tone === 'active') {
    return 'working';
  }
  if (session.tone === 'danger') {
    return 'danger';
  }
  const attentionKinds = new Set(
    session.attentionItems.map((item) => item.kind)
  );
  if (
    attentionKinds.has('recorded_wait') ||
    attentionKinds.has('plan_approval') ||
    attentionKinds.has('on_hold')
  ) {
    return 'waiting';
  }
  if (attentionKinds.has('status_check') || session.tone === 'unknown') {
    return 'unknown';
  }
  if (attentionKinds.has('awaiting_review') || session.tone === 'review') {
    return 'review';
  }
  if (
    session.jobs.some((jobView) => {
      const tone = jobPhaseTone(jobView);
      return tone === 'queued' || tone === 'review';
    })
  ) {
    return 'review';
  }
  if (session.tone === 'ok') {
    return 'completed';
  }
  return 'neutral';
}

function sessionIndicatorKind(session: ProjectSessionView): SessionIndicatorKind {
  if (session.runningCount > 0 || session.tone === 'active') {
    return 'working';
  }
  if (session.tone === 'danger') {
    return 'danger';
  }
  if (session.attention || session.tone === 'waiting') {
    return 'waiting';
  }
  if (session.tone === 'ok') {
    return 'completed';
  }
  return 'neutral';
}

function formatCallModelLine(call: ProjectCallView): string {
  const model = call.model.trim() || 'Model not reported';
  const effort = call.effort?.trim() ?? '';
  if (!effort || model.toLowerCase().includes(effort.toLowerCase())) {
    return model;
  }
  return `${model} · ${effort}`;
}

function formatFinishedDurationText(call: ProjectCallView): string {
  const state = call.stateLabel.trim();
  const duration = call.durationLabel.trim();
  if (!state) {
    return duration;
  }
  if (!duration || duration.toLowerCase().startsWith(state.toLowerCase())) {
    return duration || state;
  }
  return `${state} · ${duration}`;
}

function buildJobInspectDetail(jobView: ProjectJobView): string {
  const job = jobView.job;
  const parts: string[] = [`Job ${job.id}`, `Source ${job.source_id}`];
  if (job.plan_id) {
    parts.push(
      job.plan_revision !== null
        ? `Plan ${job.plan_id} rev ${job.plan_revision}`
        : `Plan ${job.plan_id}`
    );
  }
  parts.push(`Approval: ${job.approval_status}`);
  parts.push(`Execution: ${job.execution_status}`);
  if (job.review_state) {
    parts.push(`Review: ${job.review_state}`);
  }
  return parts.join(' · ');
}

export class ProjectsPage {
  public readonly element: HTMLElement;

  private readonly options: ProjectsPageOptions;
  private readonly storage: ProjectsArchiveStorage | null;
  private readonly liveBadgeEl: HTMLSpanElement;
  private readonly liveDotEl: HTMLSpanElement;
  private readonly liveTextEl: HTMLSpanElement;
  private readonly toolbarLabelEl: HTMLSpanElement;
  private readonly clearScopeBtn: HTMLButtonElement;
  private readonly windowSelectEl: HTMLSelectElement;
  private readonly archiveBtn: HTMLButtonElement;
  private readonly noticeBannerEl: HTMLDivElement;
  private readonly projectsContainer: HTMLElement;
  private readonly globalEmptyEl: HTMLParagraphElement;
  private readonly footerNoteLeftEl: HTMLSpanElement;
  private readonly footerNoteRightEl: HTMLSpanElement;
  private readonly loadMoreBtn: HTMLButtonElement;
  private readonly diagnosticsDetailsEl: HTMLDetailsElement;
  private readonly diagnosticsSummaryTextEl: HTMLSpanElement;
  private readonly warningsListEl: HTMLUListElement;
  private readonly announcementEl: HTMLSpanElement;

  private readonly projectEntries = new Map<string, ProjectDomEntry>();
  private readonly diagnosticEntries = new Map<string, DiagnosticDomEntry>();
  private readonly initializedProjects = new Set<string>();
  private readonly initializedSessions = new Set<string>();
  private readonly initializedRecent = new Set<string>();
  private readonly userProjectOpenState = new Map<string, boolean>();
  private readonly userSessionOpenState = new Map<string, boolean>();
  private readonly userRecentOpenState = new Map<string, boolean>();
  private readonly userOlderJobsOpenState = new Map<string, boolean>();
  private readonly userDiagnosticTechnicalOpenState = new Map<string, boolean>();
  private userDiagnosticsOpen = false;

  private windowHours: ProjectsWindowHours = 6;
  private archiveBeforeMs: number | null = null;
  private archiveError: string | null = null;
  private lastSnapshot: ObservationSnapshot | null = null;
  private currentScope: ActivityFeedScope | null = null;
  private pendingFocusScope: ActivityFeedScope | null = null;
  private focusedJobKey: string | null = null;
  private loading = false;
  private error: string | null = null;
  private hasAutoExpandedFirstProject = false;
  private disposed = false;

  public constructor(options: ProjectsPageOptions = {}) {
    this.options = options;
    this.storage =
      'storage' in options
        ? (options.storage ?? null)
        : resolveBrowserArchiveStorage();
    this.archiveBeforeMs = readProjectsArchiveCutoff(this.storage);

    this.liveDotEl = h('span', { className: 'tp-live-dot', 'aria-hidden': 'true' });
    this.liveTextEl = h('span', { className: 'tp-live-text' }, 'Waiting for data');
    this.liveBadgeEl = h(
      'span',
      { className: 'tp-live', 'data-active': 'false' },
      this.liveDotEl,
      this.liveTextEl
    );

    const headingEl = h(
      'div',
      { className: 'tp-heading' },
      h('h2', undefined, 'Projects'),
      this.liveBadgeEl
    );

    this.toolbarLabelEl = h('span', { className: 'tp-toolbar-label' }, 'Recent sessions');

    const backIcon = createUiIcon('arrow-left');
    this.clearScopeBtn = h(
      'button',
      {
        type: 'button',
        className: 'tp-back-btn cursor-interaction',
        hidden: true,
      },
      backIcon,
      h('span', undefined, 'Back to all projects')
    );
    this.clearScopeBtn.addEventListener('click', () => {
      if (this.disposed) {
        return;
      }
      this.currentScope = null;
      this.pendingFocusScope = null;
      this.focusedJobKey = null;
      this.options.onClearScope?.();
      if (this.lastSnapshot) {
        this.renderCurrentState();
      } else {
        this.syncScopeControls();
      }
    });

    this.windowSelectEl = h(
      'select',
      {
        'data-time-window': '',
        'aria-label': 'Session time window',
      },
      h('option', { value: '6', selected: true }, 'Last 6 hours'),
      h('option', { value: '24' }, 'Last 24 hours')
    );
    this.windowSelectEl.addEventListener('change', () => {
      if (this.disposed) {
        return;
      }
      const nextWindow: ProjectsWindowHours = this.windowSelectEl.value === '24' ? 24 : 6;
      this.windowHours = nextWindow;
      this.announcementEl.textContent = `Showing sessions from the last ${nextWindow} hours.`;
      if (this.lastSnapshot) {
        this.renderCurrentState();
      }
    });

    this.archiveBtn = h(
      'button',
      {
        type: 'button',
        className: 'tp-back-btn cursor-interaction',
        'data-archive-history': '',
        disabled: true,
      },
      'Archive history'
    );
    this.archiveBtn.addEventListener('click', () => {
      if (
        this.disposed ||
        this.archiveBtn.disabled ||
        this.lastSnapshot === null ||
        hasActiveScope(this.currentScope)
      ) {
        return;
      }
      const nowMs = Date.now();
      const writeResult = writeProjectsArchiveCutoff(
        this.storage,
        nowMs,
        nowMs
      );
      if (!writeResult.ok) {
        this.archiveError = writeResult.error;
        this.announcementEl.textContent = writeResult.error;
        this.syncNoticeBanner();
        return;
      }
      this.archiveError = null;
      this.archiveBeforeMs = writeResult.archiveBeforeMs;
      this.announcementEl.textContent =
        'Archived inactive project history before now. Active work remains visible.';
      this.renderCurrentState(nowMs);
    });

    const toolbarEl = h(
      'div',
      { className: 'tp-toolbar' },
      h('div', { className: 'tp-toolbar-start' }, this.toolbarLabelEl, this.clearScopeBtn),
      h(
        'div',
        { className: 'tp-toolbar-start' },
        h(
          'label',
          { className: 'tp-window-label' },
          h('span', { className: 'tp-sr-only' }, 'Session time window'),
          this.windowSelectEl
        ),
        this.archiveBtn
      )
    );

    this.noticeBannerEl = h('div', {
      className: 'tp-notice',
      role: 'status',
      hidden: true,
    });

    this.globalEmptyEl = h(
      'p',
      { className: 'tp-empty tp-empty-global', hidden: true },
      'Waiting for observation snapshot…'
    );

    this.projectsContainer = h(
      'div',
      { className: 'tp-projects', 'aria-label': 'Projects and conversations' },
      this.globalEmptyEl
    );

    this.footerNoteLeftEl = h(
      'span',
      { className: 'tp-footer-summary' },
      this.archiveBeforeMs !== null ? 'History archived' : 'Available records'
    );
    this.footerNoteRightEl = h(
      'span',
      { className: 'tp-footer-window' },
      this.archiveBeforeMs !== null ? 'Last 6h · Archived' : 'Last 6h'
    );
    this.loadMoreBtn = h(
      'button',
      {
        type: 'button',
        className: 'tp-load-more cursor-interaction',
        hidden: true,
      },
      'Load more history'
    );
    this.loadMoreBtn.addEventListener('click', () => {
      if (this.disposed || this.loadMoreBtn.disabled) {
        return;
      }
      this.options.onLoadMore?.();
    });

    const bottomNoteEl = h(
      'div',
      { className: 'tp-bottom-note' },
      this.footerNoteLeftEl,
      h('div', { className: 'tp-bottom-actions' }, this.loadMoreBtn, this.footerNoteRightEl)
    );

    this.diagnosticsSummaryTextEl = h(
      'span',
      { className: 'tp-diagnostics-title' },
      'Diagnostics (0)'
    );
    const diagnosticsChevron = createUiIcon('chevron-right');
    diagnosticsChevron.classList.add('tp-chevron');
    const diagnosticsSummaryEl = h(
      'summary',
      { className: 'tp-diagnostics-summary cursor-interaction' },
      this.diagnosticsSummaryTextEl,
      diagnosticsChevron
    );
    const diagnosticsNoteEl = h(
      'p',
      { className: 'tp-diagnostics-note' },
      'Technical notes from bounded observation queries or unavailable sources.'
    );
    this.warningsListEl = h('ul', {
      className: 'tp-warnings',
      'aria-label': 'Observation warnings',
      hidden: true,
    });
    this.diagnosticsDetailsEl = h(
      'details',
      { className: 'tp-diagnostics', 'data-diagnostics': '', hidden: true },
      diagnosticsSummaryEl,
      diagnosticsNoteEl,
      this.warningsListEl
    );
    this.diagnosticsDetailsEl.open = false;
    this.diagnosticsDetailsEl.addEventListener('toggle', () => {
      if (!this.disposed) {
        this.userDiagnosticsOpen = this.diagnosticsDetailsEl.open;
      }
    });

    this.announcementEl = h('span', {
      className: 'tp-sr-only',
      'data-window-announcement': '',
      'aria-live': 'polite',
    });

    this.element = h(
      'section',
      {
        className: 'threadlane-projects',
        'aria-label': 'Threadlane project overview',
      },
      headingEl,
      toolbarEl,
      this.noticeBannerEl,
      this.projectsContainer,
      bottomNoteEl,
      this.diagnosticsDetailsEl,
      this.announcementEl
    );
    const initialLiveTooltip = '0 running calls among recorded loaded calls in view';
    this.liveBadgeEl.setAttribute('aria-label', initialLiveTooltip);
    this.liveTextEl.textContent = '0 working in view';
    if (this.archiveBeforeMs !== null) {
      this.announcementEl.textContent = 'Archived history filter active.';
    }
  }

  public update(snapshot: ObservationSnapshot | null, options?: ProjectsUpdateOptions): void {
    if (this.disposed) {
      return;
    }

    this.loading = Boolean(options?.loading);
    this.error = options?.error ?? null;
    this.lastSnapshot = snapshot;
    if (options && 'scope' in options) {
      this.currentScope = options.scope ?? null;
    } else if (snapshot === null) {
      this.currentScope = null;
    }
    if (snapshot === null) {
      this.pendingFocusScope = null;
      this.focusedJobKey = null;
    }

    this.renderCurrentState();
  }

  public refreshTimeLabels(): void {
    if (this.disposed || this.lastSnapshot === null) {
      return;
    }
    const nowMs = Date.now();
    const projection = projectProjects(this.lastSnapshot, {
      nowMs,
      windowHours: this.windowHours,
      scope: this.currentScope,
      archiveBeforeMs: this.archiveBeforeMs,
    });

    const signature = (projects: ProjectGroupView[]): string =>
      JSON.stringify(
        projects.map((project) => ({
          key: project.key,
          runningCount: project.runningCount,
          sessions: project.sessions.map((session) => ({
            key: session.key,
            stateLabel: session.stateLabel,
            tone: session.tone,
            attention: session.attention,
            runningCount: session.runningCount,
            olderJobCount: session.olderJobCount,
            attentionItems: session.attentionItems.map((item) => ({
              key: item.key,
              kind: item.kind,
              older: item.olderThanWindow,
            })),
            jobs: session.jobs.map((job) => ({
              key: job.key,
              state: job.stateLabel,
              older: job.olderThanWindow,
            })),
            calls: session.calls.map((call) => ({
              key: call.key,
              state: call.stateLabel,
              running: call.running,
            })),
            recentEvents: session.recent.map((event) => event.key),
          })),
        }))
      );
    const currentProjects = [...this.projectEntries.values()].map((entry) => entry.project);
    if (signature(currentProjects) !== signature(projection.projects)) {
      this.renderCurrentState(nowMs);
      return;
    }

    for (const project of projection.projects) {
      const projectEntry = this.projectEntries.get(project.key);
      if (!projectEntry) {
        continue;
      }
      for (const session of project.sessions) {
        const sessionEntry = projectEntry.sessionEntries.get(session.key);
        if (!sessionEntry) {
          continue;
        }
        sessionEntry.ageEl.textContent = formatRelativeTime(
          sessionEntry.session.updatedAtMs,
          nowMs
        );
        const pendingAges =
          sessionEntry.pendingListEl.querySelectorAll<HTMLElement>('[data-recorded-ms]');
        for (const ageNode of pendingAges) {
          const raw = Number(ageNode.getAttribute('data-recorded-ms'));
          ageNode.textContent = formatRecordedAge(
            Number.isFinite(raw) && raw >= 0 ? raw : null,
            nowMs
          );
        }
        for (const jobEntry of sessionEntry.jobEntries.values()) {
          const ageText = formatRecordedAge(jobEntry.jobView.updatedAtMs, nowMs);
          jobEntry.recordedAgeEl.textContent = jobEntry.jobView.olderThanWindow
            ? `${ageText} · Outside ${this.windowHours}h window`
            : ageText;
        }
        for (const call of session.calls) {
          const callEntry = sessionEntry.callEntries.get(call.key);
          if (callEntry && call.running) {
            callEntry.call = call;
            callEntry.durationTextEl.textContent = call.durationLabel || 'Running';
          }
        }
        for (const eventEntry of sessionEntry.eventEntries.values()) {
          eventEntry.timeEl.textContent = formatRelativeTime(eventEntry.event.atMs, nowMs);
        }
      }
    }
  }

  public focusJob(scope: ActivityFeedScope): void {
    if (this.disposed) {
      return;
    }
    if (!hasActiveScope(scope)) {
      return;
    }
    this.pendingFocusScope = scope;

    if (this.tryApplyFocusScope(scope)) {
      this.pendingFocusScope = null;
      return;
    }

    if (this.lastSnapshot) {
      this.currentScope = scope;
      this.renderCurrentState();
      if (this.tryApplyFocusScope(scope)) {
        this.pendingFocusScope = null;
      }
    }
  }

  public dispose(): void {
    this.disposed = true;
    this.pendingFocusScope = null;
  }

  private syncScopeControls(): void {
    const scoped = hasActiveScope(this.currentScope);
    this.clearScopeBtn.hidden = !scoped;
    this.archiveBtn.hidden = scoped;
    this.archiveBtn.disabled = scoped || this.lastSnapshot === null;
    this.toolbarLabelEl.textContent = scoped ? 'Scoped job view' : 'Recent sessions';
  }

  private renderCurrentState(nowMs = Date.now()): void {
    const mainScrollEl = this.element.closest('.aq-main') as HTMLElement | null;
    const savedScrollTop = mainScrollEl ? mainScrollEl.scrollTop : null;

    this.syncScopeControls();
    this.syncNoticeBanner();

    if (this.lastSnapshot === null) {
      this.reconcileProjects([], nowMs);
      this.initializedProjects.clear();
      this.initializedSessions.clear();
      this.initializedRecent.clear();
      this.userProjectOpenState.clear();
      this.userSessionOpenState.clear();
      this.userRecentOpenState.clear();
      this.userOlderJobsOpenState.clear();
      this.userDiagnosticTechnicalOpenState.clear();
      this.hasAutoExpandedFirstProject = false;
      const resetTooltip = '0 running calls among recorded loaded calls in view';
      this.liveBadgeEl.setAttribute('aria-label', resetTooltip);
      this.liveBadgeEl.setAttribute('data-active', 'false');
      if (this.error) {
        this.liveTextEl.textContent = 'Unavailable';
        this.globalEmptyEl.textContent = 'Unable to load project observation snapshot.';
      } else if (this.loading) {
        this.liveTextEl.textContent = 'Loading…';
        this.globalEmptyEl.textContent = 'Loading project activity…';
      } else {
        this.liveTextEl.textContent = 'Waiting for data';
        this.globalEmptyEl.textContent = 'Waiting for observation snapshot…';
      }
      this.globalEmptyEl.hidden = false;
      this.footerNoteLeftEl.textContent = this.error
        ? 'Observation state unavailable'
        : this.archiveBeforeMs !== null
          ? 'History archived'
          : 'Available records';
      this.footerNoteRightEl.textContent =
        this.archiveBeforeMs !== null
          ? `Last ${this.windowHours}h · Archived`
          : `Last ${this.windowHours}h`;
      this.loadMoreBtn.hidden = true;
      this.diagnosticEntries.clear();
      while (this.warningsListEl.firstChild) {
        this.warningsListEl.removeChild(this.warningsListEl.firstChild);
      }
      this.warningsListEl.hidden = true;
      this.diagnosticsDetailsEl.hidden = true;
      if (mainScrollEl && savedScrollTop !== null && mainScrollEl.scrollTop !== savedScrollTop) {
        mainScrollEl.scrollTop = savedScrollTop;
      }
      return;
    }

    const projection = projectProjects(this.lastSnapshot, {
      nowMs,
      windowHours: this.windowHours,
      scope: this.currentScope,
      archiveBeforeMs: this.archiveBeforeMs,
    });

    this.syncHeaderRunningCount(projection);
    this.reconcileProjects(projection.projects, nowMs);

    if (projection.projects.length === 0) {
      this.globalEmptyEl.textContent =
        projection.emptyMessage ?? 'No conversations active in the selected time window.';
      this.globalEmptyEl.hidden = false;
    } else {
      this.globalEmptyEl.hidden = true;
    }

    this.syncFooterAndWarnings(projection);

    const focusApplied = Boolean(
      this.pendingFocusScope && this.tryApplyFocusScope(this.pendingFocusScope)
    );
    if (focusApplied) {
      this.pendingFocusScope = null;
    }

    if (
      !focusApplied &&
      mainScrollEl &&
      savedScrollTop !== null &&
      mainScrollEl.scrollTop !== savedScrollTop
    ) {
      mainScrollEl.scrollTop = savedScrollTop;
    }
  }

  private syncNoticeBanner(): void {
    if (this.error) {
      this.noticeBannerEl.className = 'tp-notice tp-notice-error';
      this.noticeBannerEl.setAttribute('role', 'alert');
      const backendNotice =
        this.lastSnapshot !== null
          ? `${this.error} Showing last recorded snapshot.`
          : this.error;
      this.noticeBannerEl.textContent = this.archiveError
        ? `${backendNotice} ${this.archiveError}`
        : backendNotice;
      this.noticeBannerEl.hidden = false;
      return;
    }
    if (this.archiveError) {
      this.noticeBannerEl.className = 'tp-notice tp-notice-error';
      this.noticeBannerEl.setAttribute('role', 'alert');
      this.noticeBannerEl.textContent = this.archiveError;
      this.noticeBannerEl.hidden = false;
      return;
    }

    if (this.loading && this.lastSnapshot === null) {
      this.noticeBannerEl.className = 'tp-notice';
      this.noticeBannerEl.setAttribute('role', 'status');
      this.noticeBannerEl.textContent = 'Loading project observation state…';
      this.noticeBannerEl.hidden = false;
      return;
    }

    this.noticeBannerEl.hidden = true;
    this.noticeBannerEl.textContent = '';
  }

  private syncHeaderRunningCount(projection: ProjectsProjection): void {
    const liveTooltip =
      projection.runningCount === 1
        ? '1 running call among recorded loaded calls in view'
        : `${projection.runningCount} running calls among recorded loaded calls in view`;
    this.liveBadgeEl.setAttribute('aria-label', liveTooltip);
    if (projection.runningCount > 0) {
      this.liveBadgeEl.setAttribute('data-active', 'true');
      this.liveTextEl.textContent =
        projection.runningCount === 1
          ? '1 agent working'
          : `${projection.runningCount} agents working`;
    } else {
      this.liveBadgeEl.setAttribute('data-active', 'false');
      this.liveTextEl.textContent = '0 working in view';
    }
  }

  private syncFooterAndWarnings(projection: ProjectsProjection): void {
    const summaryParts: string[] = [];
    if (
      this.archiveBeforeMs !== null &&
      !hasActiveScope(this.currentScope)
    ) {
      summaryParts.push('History archived');
    }
    if (projection.hiddenSessionCount > 0) {
      summaryParts.push(
        `${projection.hiddenSessionCount} older ${
          projection.hiddenSessionCount === 1 ? 'session' : 'sessions'
        } outside ${this.windowHours}h window`
      );
    }
    if (projection.unavailableSourceCount > 0) {
      summaryParts.push(
        `${projection.unavailableSourceCount} ${
          projection.unavailableSourceCount === 1 ? 'source' : 'sources'
        } unavailable`
      );
    }
    if (projection.hasMore || projection.jobsHasMore) {
      summaryParts.push('Partial history loaded');
    }
    if (this.loading && this.lastSnapshot !== null) {
      summaryParts.push('Refreshing…');
    }

    this.footerNoteLeftEl.textContent =
      summaryParts.length > 0 ? summaryParts.join(' · ') : 'Available records';

    const showLoadMore = Boolean(
      projection.hasMore &&
        this.lastSnapshot?.next_cursor &&
        this.lastSnapshot.next_cursor.trim().length > 0
    );
    this.loadMoreBtn.hidden = !showLoadMore;
    this.loadMoreBtn.disabled = !showLoadMore || this.loading || !this.options.onLoadMore;
    this.footerNoteRightEl.textContent = hasActiveScope(this.currentScope)
      ? 'Scoped view'
      : this.archiveBeforeMs !== null
        ? `Last ${this.windowHours}h · Archived`
        : `Last ${this.windowHours}h`;

    this.reconcileDiagnostics(projection.diagnostics);
  }

  private reconcileDiagnostics(diagnostics: ProjectDiagnosticRowView[]): void {
    const activeEl =
      typeof document !== 'undefined' ? document.activeElement : null;
    let focusedDiagnosticKey: string | null = null;
    let focusedDiagnosticControl: 'summary' | 'details' | 'item' | null = null;
    if (activeEl) {
      for (const [key, entry] of this.diagnosticEntries.entries()) {
        if (
          activeEl === entry.technicalSummaryEl ||
          entry.technicalSummaryEl.contains(activeEl)
        ) {
          focusedDiagnosticKey = key;
          focusedDiagnosticControl = 'summary';
          break;
        }
        if (activeEl === entry.technicalDetailsEl) {
          focusedDiagnosticKey = key;
          focusedDiagnosticControl = 'details';
          break;
        }
        if (activeEl === entry.element || entry.element.contains(activeEl)) {
          focusedDiagnosticKey = key;
          focusedDiagnosticControl = 'item';
          break;
        }
      }
    }

    const nextKeys = new Set(diagnostics.map((diag) => diag.key));
    for (const [key, entry] of this.diagnosticEntries.entries()) {
      if (!nextKeys.has(key)) {
        this.userDiagnosticTechnicalOpenState.set(
          key,
          entry.technicalDetailsEl.open
        );
        entry.element.remove();
        this.diagnosticEntries.delete(key);
      }
    }

    if (diagnostics.length === 0) {
      this.diagnosticsSummaryTextEl.textContent = 'Diagnostics (0)';
      this.warningsListEl.hidden = true;
      this.diagnosticsDetailsEl.hidden = true;
      return;
    }

    this.diagnosticsSummaryTextEl.textContent = `Diagnostics (${diagnostics.length})`;
    this.warningsListEl.hidden = false;
    this.diagnosticsDetailsEl.hidden = false;
    this.diagnosticsDetailsEl.open = this.userDiagnosticsOpen;

    for (let index = 0; index < diagnostics.length; index += 1) {
      const diag = diagnostics[index]!;
      let entry = this.diagnosticEntries.get(diag.key);
      if (!entry) {
        entry = this.createDiagnosticEntry(diag);
        this.diagnosticEntries.set(diag.key, entry);
      } else {
        this.userDiagnosticTechnicalOpenState.set(
          diag.key,
          entry.technicalDetailsEl.open
        );
        this.updateDiagnosticEntry(entry, diag);
      }

      const targetNode = this.warningsListEl.children[index] ?? null;
      if (targetNode !== entry.element) {
        this.warningsListEl.insertBefore(entry.element, targetNode);
      }
    }

    if (focusedDiagnosticKey !== null && focusedDiagnosticControl !== null) {
      const retainedEntry = this.diagnosticEntries.get(focusedDiagnosticKey);
      if (retainedEntry) {
        const targetFocusEl =
          focusedDiagnosticControl === 'summary'
            ? retainedEntry.technicalSummaryEl
            : focusedDiagnosticControl === 'details'
              ? retainedEntry.technicalDetailsEl
              : retainedEntry.element;
        if (
          typeof document !== 'undefined' &&
          document.activeElement !== targetFocusEl &&
          typeof targetFocusEl.focus === 'function'
        ) {
          targetFocusEl.focus({ preventScroll: true });
        }
      }
    }
  }

  private createDiagnosticEntry(diag: ProjectDiagnosticRowView): DiagnosticDomEntry {
    const dotEl = h('span', {
      className:
        diag.severity === 'warning'
          ? 'tp-dot waiting tp-diagnostic-dot'
          : 'tp-dot tp-diagnostic-dot',
      'aria-hidden': 'true',
    });
    const titleEl = h('span', { className: 'tp-diagnostic-item-title' }, diag.title);
    const headerEl = h('div', { className: 'tp-diagnostic-header' }, dotEl, titleEl);

    const metaEl = h(
      'p',
      { className: 'tp-diagnostic-meta' },
      `${diag.sourceLabel} · ${diag.area}`
    );
    const impactEl = h('p', { className: 'tp-diagnostic-impact' }, diag.impact);
    const nextStepEl = h('p', { className: 'tp-diagnostic-next' }, diag.nextStep);
    const scopeNoteEl = h(
      'p',
      { className: 'tp-diagnostic-scope' },
      diag.conversationNote
    );

    const technicalChevron = createUiIcon('chevron-right');
    technicalChevron.classList.add('tp-chevron');
    const technicalSummaryEl = h(
      'summary',
      { className: 'tp-diagnostic-technical-summary cursor-interaction' },
      h('span', undefined, 'Technical details'),
      technicalChevron
    );
    const technicalTextEl = h(
      'p',
      { className: 'tp-diagnostic-technical-text' },
      diag.technicalDetails
    );
    const technicalDetailsEl = h(
      'details',
      { className: 'tp-diagnostic-technical' },
      technicalSummaryEl,
      technicalTextEl
    );
    technicalDetailsEl.open =
      this.userDiagnosticTechnicalOpenState.get(diag.key) ?? false;
    technicalDetailsEl.addEventListener('toggle', () => {
      if (!this.disposed) {
        this.userDiagnosticTechnicalOpenState.set(diag.key, technicalDetailsEl.open);
      }
    });

    const element = h(
      'li',
      {
        className: 'tp-warning-item tp-diagnostic-item',
        'data-severity': diag.severity,
      },
      headerEl,
      metaEl,
      impactEl,
      nextStepEl,
      scopeNoteEl,
      technicalDetailsEl
    );

    return {
      key: diag.key,
      diag,
      element,
      dotEl,
      titleEl,
      metaEl,
      impactEl,
      nextStepEl,
      scopeNoteEl,
      technicalDetailsEl,
      technicalSummaryEl,
      technicalTextEl,
    };
  }

  private updateDiagnosticEntry(
    entry: DiagnosticDomEntry,
    diag: ProjectDiagnosticRowView
  ): void {
    entry.diag = diag;
    entry.element.setAttribute('data-severity', diag.severity);
    entry.dotEl.className =
      diag.severity === 'warning'
        ? 'tp-dot waiting tp-diagnostic-dot'
        : 'tp-dot tp-diagnostic-dot';
    entry.titleEl.textContent = diag.title;
    entry.metaEl.textContent = `${diag.sourceLabel} · ${diag.area}`;
    entry.impactEl.textContent = diag.impact;
    entry.nextStepEl.textContent = diag.nextStep;
    entry.scopeNoteEl.textContent = diag.conversationNote;
    entry.technicalTextEl.textContent = diag.technicalDetails;
    const savedOpen = this.userDiagnosticTechnicalOpenState.get(diag.key);
    if (savedOpen !== undefined && entry.technicalDetailsEl.open !== savedOpen) {
      entry.technicalDetailsEl.open = savedOpen;
    }
  }

  private reconcileProjects(projects: ProjectGroupView[], nowMs: number): void {
    const nextKeys = new Set(projects.map((project) => project.key));

    for (const [key, entry] of this.projectEntries.entries()) {
      if (!nextKeys.has(key)) {
        this.userProjectOpenState.set(key, entry.details.open);
        for (const [sKey, sEntry] of entry.sessionEntries.entries()) {
          this.userSessionOpenState.set(sKey, sEntry.details.open);
          this.userRecentOpenState.set(sKey, sEntry.recentDetails.open);
          this.userOlderJobsOpenState.set(sKey, sEntry.olderJobsDetails.open);
        }
        entry.details.remove();
        this.projectEntries.delete(key);
      }
    }

    for (let index = 0; index < projects.length; index += 1) {
      const project = projects[index];
      let entry = this.projectEntries.get(project.key);
      if (!entry) {
        entry = this.createProjectEntry(project, index);
        this.projectEntries.set(project.key, entry);
      } else {
        this.userProjectOpenState.set(project.key, entry.details.open);
      }

      this.updateProjectEntry(entry, project, nowMs);

      const targetNode = this.projectsContainer.children[index] ?? null;
      if (targetNode !== entry.details) {
        this.projectsContainer.insertBefore(entry.details, targetNode);
      }
    }
  }

  private createProjectEntry(project: ProjectGroupView, index: number): ProjectDomEntry {
    const iconSpan = h(
      'span',
      { className: 'tp-project-icon' },
      createUiIcon('folder-open' as UiIconName)
    );
    const nameEl = h('span', { className: 'tp-project-name' }, project.label);
    const statusEl = h('span', { className: 'tp-project-status', 'data-session-count': '' });
    const chevronEl = createUiIcon('chevron-right');
    chevronEl.classList.add('tp-chevron');

    const summary = h(
      'summary',
      { className: 'tp-project-heading cursor-interaction' },
      iconSpan,
      nameEl,
      statusEl,
      chevronEl
    );

    const emptyEl = h(
      'p',
      { className: 'tp-empty', hidden: true },
      'No conversations active in the selected time window.'
    );
    const sessionsContainer = h('div', { className: 'tp-sessions' }, emptyEl);
    const details = h(
      'details',
      { className: 'tp-project', 'data-project': project.key },
      summary,
      sessionsContainer
    );

    const savedOpen = this.userProjectOpenState.get(project.key);
    if (savedOpen !== undefined) {
      details.open = savedOpen;
    } else if (!this.initializedProjects.has(project.key)) {
      this.initializedProjects.add(project.key);
      const shouldOpen = !this.hasAutoExpandedFirstProject && index === 0;
      if (shouldOpen) {
        this.hasAutoExpandedFirstProject = true;
      }
      details.open = shouldOpen;
      this.userProjectOpenState.set(project.key, shouldOpen);
    }

    details.addEventListener('toggle', () => {
      if (!this.disposed) {
        this.userProjectOpenState.set(project.key, details.open);
      }
    });

    return {
      key: project.key,
      project,
      details,
      summary,
      nameEl,
      statusEl,
      sessionsContainer,
      emptyEl,
      sessionEntries: new Map<string, SessionDomEntry>(),
    };
  }

  private updateProjectEntry(
    entry: ProjectDomEntry,
    project: ProjectGroupView,
    nowMs: number
  ): void {
    entry.project = project;
    entry.nameEl.textContent = project.label;

    if (project.runningCount > 0) {
      entry.statusEl.className = 'tp-project-status tp-working';
      entry.statusEl.textContent = `${project.runningCount} working`;
    } else if (project.sessions.length > 0) {
      entry.statusEl.className = 'tp-project-status';
      entry.statusEl.textContent = `${project.sessions.length} recent`;
    } else {
      entry.statusEl.className = 'tp-project-status';
      entry.statusEl.textContent = 'Quiet';
    }

    this.reconcileSessions(entry, project.sessions, nowMs);
    entry.emptyEl.hidden = project.sessions.length > 0;
  }

  private reconcileSessions(
    projectEntry: ProjectDomEntry,
    sessions: ProjectSessionView[],
    nowMs: number
  ): void {
    const nextKeys = new Set(sessions.map((session) => session.key));

    for (const [key, sessionEntry] of projectEntry.sessionEntries.entries()) {
      if (!nextKeys.has(key)) {
        this.userSessionOpenState.set(key, sessionEntry.details.open);
        this.userRecentOpenState.set(key, sessionEntry.recentDetails.open);
        this.userOlderJobsOpenState.set(key, sessionEntry.olderJobsDetails.open);
        sessionEntry.details.remove();
        projectEntry.sessionEntries.delete(key);
      }
    }

    for (let index = 0; index < sessions.length; index += 1) {
      const session = sessions[index];
      let sessionEntry = projectEntry.sessionEntries.get(session.key);
      if (!sessionEntry) {
        sessionEntry = this.createSessionEntry(session);
        projectEntry.sessionEntries.set(session.key, sessionEntry);
      } else {
        this.userSessionOpenState.set(session.key, sessionEntry.details.open);
        this.userRecentOpenState.set(session.key, sessionEntry.recentDetails.open);
        this.userOlderJobsOpenState.set(session.key, sessionEntry.olderJobsDetails.open);
      }

      this.updateSessionEntry(sessionEntry, session, nowMs);

      const targetNode = projectEntry.sessionsContainer.children[index] ?? projectEntry.emptyEl;
      if (targetNode !== sessionEntry.details) {
        projectEntry.sessionsContainer.insertBefore(sessionEntry.details, targetNode);
      }
    }
  }

  private createSessionEntry(session: ProjectSessionView): SessionDomEntry {
    const chatIconSpan = h(
      'span',
      { className: 'tp-chat-icon' },
      createUiIcon('messages-square')
    );
    const nameEl = h('span', { className: 'tp-session-name' }, session.label);
    const stateTextNode = document.createTextNode(session.stateLabel);
    const stateEl = h('span', { className: 'tp-session-state' }, stateTextNode);
    const copyEl = h('span', { className: 'tp-session-copy' }, nameEl, stateEl);
    const ageEl = h('span', { className: 'tp-session-age' });
    const detailMetaEl = h('span', { className: 'tp-sr-only tp-session-detail', hidden: true });

    const summary = h(
      'summary',
      { className: 'tp-session-heading cursor-interaction' },
      chatIconSpan,
      copyEl,
      ageEl,
      detailMetaEl
    );

    const pendingHeadingEl = h(
      'div',
      { className: 'tp-pending-heading' },
      'Pending items'
    );
    const pendingSummaryEl = h('p', {
      className: 'tp-pending-summary',
      hidden: true,
    });
    const pendingListEl = h('ul', { className: 'tp-pending-list' });
    const pendingSectionEl = h(
      'div',
      {
        className: 'tp-pending-section',
        'data-pending-section': session.key,
        hidden: true,
      },
      pendingHeadingEl,
      pendingSummaryEl,
      pendingListEl
    );

    const olderJobsSummaryTextEl = h(
      'span',
      { className: 'tp-older-jobs-title' },
      'Older unresolved work (0)'
    );
    const olderJobsChevron = createUiIcon('chevron-right');
    olderJobsChevron.classList.add('tp-chevron');
    const olderJobsSummaryEl = h(
      'summary',
      { className: 'tp-older-jobs-summary cursor-interaction' },
      olderJobsSummaryTextEl,
      olderJobsChevron
    );
    const olderJobsListEl = h('div', { className: 'tp-older-jobs-list' });
    const olderJobsDetails = h(
      'details',
      {
        className: 'tp-older-jobs',
        'data-older-jobs': session.key,
        hidden: true,
      },
      olderJobsSummaryEl,
      olderJobsListEl
    );
    const savedOlderOpen = this.userOlderJobsOpenState.get(session.key);
    olderJobsDetails.open = savedOlderOpen ?? false;
    olderJobsDetails.addEventListener('toggle', () => {
      if (!this.disposed) {
        this.userOlderJobsOpenState.set(session.key, olderJobsDetails.open);
      }
    });

    const jobsContainer = h('div', { className: 'tp-jobs' }, olderJobsDetails);
    const callsContainer = h('div', {
      className: 'tp-calls',
      'aria-label': 'Agent calls',
      hidden: true,
    });

    const recentChevron = createUiIcon('chevron-right');
    const recentSummary = h(
      'summary',
      { className: 'cursor-interaction' },
      h('span', undefined, 'Recent activity'),
      recentChevron
    );
    const eventsList = h('ol', { className: 'tp-events' });
    const recentDetails = h(
      'details',
      { className: 'tp-recent', 'data-recent': session.key },
      recentSummary,
      eventsList
    );

    const savedRecentOpen = this.userRecentOpenState.get(session.key);
    if (savedRecentOpen !== undefined) {
      recentDetails.open = savedRecentOpen;
    } else if (!this.initializedRecent.has(session.key)) {
      this.initializedRecent.add(session.key);
      const openRecentByDefault = session.runningCount > 0 || session.jobs.length === 0;
      recentDetails.open = openRecentByDefault;
      this.userRecentOpenState.set(session.key, openRecentByDefault);
    }
    recentDetails.addEventListener('toggle', () => {
      if (!this.disposed) {
        this.userRecentOpenState.set(session.key, recentDetails.open);
      }
    });

    const quietNoteEl = h('p', { className: 'tp-context-note', hidden: true });
    const bodyEl = h(
      'div',
      { className: 'tp-session-body' },
      pendingSectionEl,
      callsContainer,
      jobsContainer,
      quietNoteEl,
      recentDetails
    );

    const details = h(
      'details',
      { className: 'tp-session', 'data-session': session.key },
      summary,
      bodyEl
    );

    const savedSessionOpen = this.userSessionOpenState.get(session.key);
    if (savedSessionOpen !== undefined) {
      details.open = savedSessionOpen;
    } else if (!this.initializedSessions.has(session.key)) {
      this.initializedSessions.add(session.key);
      const shouldOpen = session.runningCount > 0;
      details.open = shouldOpen;
      this.userSessionOpenState.set(session.key, shouldOpen);
    }
    details.addEventListener('toggle', () => {
      if (!this.disposed) {
        this.userSessionOpenState.set(session.key, details.open);
      }
    });

    return {
      key: session.key,
      session,
      details,
      summary,
      nameEl,
      stateEl,
      indicatorKind: null,
      stateTextNode,
      ageEl,
      detailMetaEl,
      bodyEl,
      pendingSectionEl,
      pendingHeadingEl,
      pendingSummaryEl,
      pendingListEl,
      jobsContainer,
      olderJobsDetails,
      olderJobsSummaryTextEl,
      olderJobsListEl,
      jobEntries: new Map<string, JobDomEntry>(),
      callsContainer,
      callEntries: new Map<string, CallDomEntry>(),
      recentDetails,
      eventsList,
      eventEntries: new Map<string, EventDomEntry>(),
      quietNoteEl,
    };
  }

  private updateSessionEntry(
    entry: SessionDomEntry,
    session: ProjectSessionView,
    nowMs: number
  ): void {
    entry.session = session;
    entry.details.setAttribute('data-active', session.runningCount > 0 ? 'true' : 'false');
    entry.details.setAttribute('data-attention', session.attention ? 'true' : 'false');
    if (session.threadUuid) {
      entry.details.setAttribute('data-thread-uuid', session.threadUuid);
    } else {
      entry.details.removeAttribute('data-thread-uuid');
    }
    if (session.bindingId) {
      entry.details.setAttribute('data-binding-id', session.bindingId);
    } else {
      entry.details.removeAttribute('data-binding-id');
    }

    entry.nameEl.textContent = session.label;
    entry.detailMetaEl.textContent = session.detail;
    entry.stateEl.setAttribute('data-tone', sessionStatusTone(session));

    const nextIndicator = sessionIndicatorKind(session);
    if (entry.indicatorKind !== nextIndicator) {
      entry.indicatorKind = nextIndicator;
      while (entry.stateEl.firstChild && entry.stateEl.firstChild !== entry.stateTextNode) {
        entry.stateEl.removeChild(entry.stateEl.firstChild);
      }
      entry.stateEl.insertBefore(
        h('span', { className: 'tp-dot', 'aria-hidden': 'true' }),
        entry.stateTextNode
      );
    }
    entry.stateTextNode.nodeValue = session.stateLabel;
    entry.ageEl.textContent = formatRelativeTime(session.updatedAtMs, nowMs);

    this.reconcilePendingItems(entry, session, nowMs);
    this.reconcileJobs(entry, session, nowMs);
    this.reconcileCalls(entry, session.calls, nowMs);
    this.reconcileEvents(entry, session.recent, nowMs);

    if (
      session.jobs.length === 0 &&
      session.calls.length === 0 &&
      session.recent.length === 0 &&
      session.attentionItems.length === 0
    ) {
      entry.quietNoteEl.textContent = session.detail || 'No active agents in this conversation.';
      entry.quietNoteEl.hidden = false;
    } else {
      entry.quietNoteEl.hidden = true;
    }
  }

  private reconcilePendingItems(
    sessionEntry: SessionDomEntry,
    session: ProjectSessionView,
    nowMs: number
  ): void {
    // Job-derived items duplicate their own job disclosure; request-derived
    // items (including linked requests) still need their reason and age.
    const items = session.attentionItems.filter(
      (item) => item.requestId !== null || item.jobId === null
    );
    if (items.length === 0) {
      if (pendingListSignatures.get(sessionEntry) !== '[]') {
        while (sessionEntry.pendingListEl.firstChild) {
          sessionEntry.pendingListEl.removeChild(sessionEntry.pendingListEl.firstChild);
        }
        pendingListSignatures.set(sessionEntry, '[]');
      }
      sessionEntry.pendingSectionEl.hidden = true;
      sessionEntry.pendingSummaryEl.hidden = true;
      sessionEntry.pendingSummaryEl.textContent = '';
      return;
    }

    const signature = JSON.stringify(
      items.map((item) => [
        item.key,
        item.kind,
        item.sourceId,
        item.jobId,
        item.title,
        item.reason,
        item.nextStep,
        item.olderThanWindow ? 'older' : 'current',
      ])
    );
    if (pendingListSignatures.get(sessionEntry) === signature) {
      const ageNodes =
        sessionEntry.pendingListEl.querySelectorAll<HTMLElement>(
          '[data-recorded-ms]'
        );
      for (let index = 0; index < items.length; index += 1) {
        const ageNode = ageNodes[index];
        const item = items[index];
        if (!ageNode || !item) {
          continue;
        }
        ageNode.setAttribute(
          'data-recorded-ms',
          item.updatedAtMs !== null ? String(item.updatedAtMs) : ''
        );
        ageNode.textContent = formatRecordedAge(item.updatedAtMs, nowMs);
      }
      return;
    }
    pendingListSignatures.set(sessionEntry, signature);
    while (sessionEntry.pendingListEl.firstChild) {
      sessionEntry.pendingListEl.removeChild(sessionEntry.pendingListEl.firstChild);
    }

    const allOlder = items.every((item) => item.olderThanWindow);
    sessionEntry.pendingHeadingEl.textContent = allOlder
      ? 'Older unresolved work'
      : 'Pending items';
    const breakdown = summarizeAttentionBreakdown(items);
    if (items.length > 1 && breakdown) {
      sessionEntry.pendingSummaryEl.textContent = breakdown;
      sessionEntry.pendingSummaryEl.hidden = false;
    } else {
      sessionEntry.pendingSummaryEl.hidden = true;
      sessionEntry.pendingSummaryEl.textContent = '';
    }

    for (const item of items) {
      const titleEl = h('span', { className: 'tp-pending-title' }, item.title);
      const ageEl = h(
        'time',
        {
          className: 'tp-pending-age',
          'data-recorded-ms': item.updatedAtMs !== null ? String(item.updatedAtMs) : '',
        },
        formatRecordedAge(item.updatedAtMs, nowMs)
      );
      const metaEl = h('span', { className: 'tp-pending-meta' }, ageEl);
      if (item.jobId !== null) {
        const targetSource = item.sourceId;
        const targetJob = item.jobId;
        const jumpBtn = h(
          'button',
          {
            type: 'button',
            className: 'tp-pending-jump cursor-interaction',
            'data-focus-source-id': targetSource,
            'data-focus-job-id': targetJob,
          },
          'Inspect job'
        );
        jumpBtn.addEventListener('click', () => {
          if (!this.disposed) {
            this.focusJob({ source_id: targetSource, job_id: targetJob });
          }
        });
        metaEl.appendChild(jumpBtn);
      }
      const topRowEl = h('div', { className: 'tp-pending-row' }, titleEl, metaEl);
      const detailEl = h(
        'div',
        { className: 'tp-pending-detail' },
        `${item.reason}. ${item.nextStep}`
      );
      sessionEntry.pendingListEl.appendChild(
        h(
          'li',
          {
            className: 'tp-pending-item',
            'data-attention-key': item.key,
            'data-attention-kind': item.kind,
            'data-older': item.olderThanWindow ? 'true' : 'false',
          },
          topRowEl,
          detailEl
        )
      );
    }
    sessionEntry.pendingSectionEl.hidden = false;
  }

  private reconcileJobs(
    sessionEntry: SessionDomEntry,
    session: ProjectSessionView,
    nowMs: number
  ): void {
    const jobs = session.jobs;
    const nextKeys = new Set(jobs.map((job) => job.key));

    for (const [key, jobEntry] of sessionEntry.jobEntries.entries()) {
      if (!nextKeys.has(key)) {
        jobEntry.element.remove();
        sessionEntry.jobEntries.delete(key);
      }
    }

    sessionEntry.jobsContainer.hidden = jobs.length === 0;
    const currentJobs = jobs.filter((job) => !job.olderThanWindow);
    const olderJobs = jobs.filter((job) => job.olderThanWindow);

    for (let index = 0; index < currentJobs.length; index += 1) {
      const jobView = currentJobs[index];
      let jobEntry = sessionEntry.jobEntries.get(jobView.key);
      if (!jobEntry) {
        jobEntry = this.createJobEntry(jobView);
        sessionEntry.jobEntries.set(jobView.key, jobEntry);
      }
      this.updateJobEntry(jobEntry, jobView, session.label, nowMs);

      const targetNode =
        sessionEntry.jobsContainer.children[index] ?? sessionEntry.olderJobsDetails;
      if (targetNode !== jobEntry.element) {
        sessionEntry.jobsContainer.insertBefore(jobEntry.element, targetNode);
      }
    }

    if (olderJobs.length > 0) {
      sessionEntry.olderJobsSummaryTextEl.textContent = `Older work (${olderJobs.length})`;
      sessionEntry.olderJobsDetails.hidden = false;
      const savedOlderOpen = this.userOlderJobsOpenState.get(session.key);
      if (savedOlderOpen !== undefined) {
        sessionEntry.olderJobsDetails.open = savedOlderOpen;
      }
      for (let index = 0; index < olderJobs.length; index += 1) {
        const jobView = olderJobs[index];
        let jobEntry = sessionEntry.jobEntries.get(jobView.key);
        if (!jobEntry) {
          jobEntry = this.createJobEntry(jobView);
          sessionEntry.jobEntries.set(jobView.key, jobEntry);
        }
        this.updateJobEntry(jobEntry, jobView, session.label, nowMs);

        const targetNode = sessionEntry.olderJobsListEl.children[index] ?? null;
        if (targetNode !== jobEntry.element) {
          sessionEntry.olderJobsListEl.insertBefore(jobEntry.element, targetNode);
        }
      }
      if (sessionEntry.jobsContainer.lastElementChild !== sessionEntry.olderJobsDetails) {
        sessionEntry.jobsContainer.appendChild(sessionEntry.olderJobsDetails);
      }
    } else {
      sessionEntry.olderJobsDetails.hidden = true;
    }
  }

  private createJobEntry(jobView: ProjectJobView): JobDomEntry {
    const jobIconSpan = h(
      'span',
      { className: 'tp-job-icon' },
      createUiIcon('clipboard-list')
    );
    const titleEl = h('span', { className: 'tp-job-title' });
    const phaseStateEl = h('span', { className: 'tp-phase-state tp-job-state' });
    const chevronEl = createUiIcon('chevron-right');
    chevronEl.classList.add('tp-chevron');

    const jobSummaryEl = h(
      'summary',
      { className: 'tp-job-summary cursor-interaction' },
      jobIconSpan,
      titleEl,
      phaseStateEl,
      chevronEl
    );

    const fullTitleEl = h('p', { className: 'tp-job-full-title', hidden: true });
    const recordedAgeEl = h('span', { className: 'tp-job-age' });
    const planLabelEl = h('span', { className: 'tp-plan-label', hidden: true });
    const phaseRightEl = h('span', { className: 'tp-phase-right' });
    const phaseEl = h('div', { className: 'tp-phase' }, planLabelEl, phaseRightEl);
    const summaryEl = h('p', { className: 'tp-plan-summary', hidden: true });
    const tasksListEl = h('ul', { className: 'tp-plan-tasks', hidden: true });
    const approvalNoteEl = h(
      'p',
      { className: 'tp-context-note tp-approval-note', hidden: true },
      'Review and approve the plan in your Codex conversation.'
    );
    const bodyEl = h(
      'div',
      { className: 'tp-job-body' },
      fullTitleEl,
      recordedAgeEl,
      phaseEl,
      summaryEl,
      tasksListEl,
      approvalNoteEl
    );
    const detailMetaEl = h('span', { className: 'tp-sr-only tp-job-detail', hidden: true });

    const element = h(
      'details',
      {
        className: 'tp-job',
        tabindex: '-1',
        'data-job-key': jobView.key,
      },
      jobSummaryEl,
      bodyEl,
      detailMetaEl
    );

    const jobOpenState = getUserJobOpenState(this);
    const savedOpen = jobOpenState.get(jobView.key);
    element.open = savedOpen ?? false;
    element.addEventListener('toggle', () => {
      if (!this.disposed) {
        jobOpenState.set(jobView.key, element.open);
      }
    });

    return {
      key: jobView.key,
      jobView,
      element,
      phaseStateEl,
      planLabelEl,
      phaseRightEl,
      titleEl,
      fullTitleEl,
      recordedAgeEl,
      summaryEl,
      tasksListEl,
      approvalNoteEl,
      detailMetaEl,
    };
  }

  private updateJobEntry(
    entry: JobDomEntry,
    jobView: ProjectJobView,
    sessionLabel: string,
    nowMs = Date.now()
  ): void {
    entry.jobView = jobView;
    const job = jobView.job;
    const inspectDetail = buildJobInspectDetail(jobView);
    entry.element.setAttribute('data-source-id', job.source_id);
    entry.element.setAttribute('data-job-id', job.id);
    entry.element.setAttribute('data-older', jobView.olderThanWindow ? 'true' : 'false');
    if (jobView.attentionKind) {
      entry.element.setAttribute('data-attention-kind', jobView.attentionKind);
    } else {
      entry.element.removeAttribute('data-attention-kind');
    }
    entry.detailMetaEl.textContent = inspectDetail;

    const isFocused = this.focusedJobKey === jobView.key;
    entry.element.classList.toggle('tp-job-focused', isFocused);
    if (isFocused) {
      entry.element.setAttribute('data-focused', 'true');
    } else {
      entry.element.removeAttribute('data-focused');
    }

    entry.element.setAttribute('data-attention', jobView.attention ? 'true' : 'false');
    entry.phaseStateEl.textContent = jobView.stateLabel;
    entry.phaseStateEl.className = 'tp-phase-state tp-job-state';
    entry.phaseStateEl.setAttribute('data-tone', jobPhaseTone(jobView));
    if (jobView.planLabel) {
      entry.planLabelEl.textContent = jobView.planLabel;
      entry.planLabelEl.hidden = false;
    } else {
      entry.planLabelEl.hidden = true;
      entry.planLabelEl.textContent = '';
    }

    const ageText = formatRecordedAge(jobView.updatedAtMs, nowMs);
    entry.recordedAgeEl.textContent = jobView.olderThanWindow
      ? `${ageText} · Outside ${this.windowHours}h window`
      : ageText;

    if (jobView.attention) {
      entry.phaseRightEl.className = 'tp-phase-right tp-waiting';
      const summary = jobView.taskSummary.trim();
      const reason = jobView.reason ?? jobView.stateLabel;
      entry.phaseRightEl.textContent =
        summary && !summary.toLowerCase().includes(reason.toLowerCase())
          ? `${reason} · ${summary}`
          : reason;
      entry.approvalNoteEl.textContent =
        jobView.nextStep ?? 'Check or continue in your Codex conversation.';
      entry.approvalNoteEl.hidden = false;
    } else {
      entry.phaseRightEl.className = 'tp-phase-right';
      entry.phaseRightEl.textContent = jobView.taskSummary;
      entry.approvalNoteEl.hidden = true;
    }
    if (entry.phaseRightEl.parentElement) {
      entry.phaseRightEl.parentElement.hidden =
        !entry.planLabelEl.textContent && !entry.phaseRightEl.textContent;
    }

    const trimmedTitle = job.title.trim();
    const displayTitle =
      trimmedTitle || sessionLabel.trim() || jobView.planLabel || `Job ${job.id}`;
    entry.titleEl.textContent = displayTitle;
    entry.titleEl.hidden = false;
    entry.fullTitleEl.textContent = displayTitle;
    entry.fullTitleEl.hidden = false;

    const trimmedSummary = job.summary?.trim() ?? '';
    if (trimmedSummary && trimmedSummary !== displayTitle) {
      entry.summaryEl.textContent = trimmedSummary;
      entry.summaryEl.hidden = false;
    } else {
      entry.summaryEl.hidden = true;
      entry.summaryEl.textContent = '';
    }

    while (entry.tasksListEl.firstChild) {
      entry.tasksListEl.removeChild(entry.tasksListEl.firstChild);
    }
    if (job.tasks.length > 0) {
      for (const task of job.tasks) {
        const statusInfo = projectTaskStatus(task.status);
        entry.tasksListEl.appendChild(
          h(
            'li',
            {
              className: 'tp-plan-task',
              'data-task-status': task.status,
            },
            h('span', { className: 'tp-task-title' }, task.title),
            h(
              'span',
              {
                className: 'tp-task-status',
                'data-tone': statusInfo.tone,
              },
              statusInfo.label
            )
          )
        );
      }
      entry.tasksListEl.hidden = false;
    } else {
      entry.tasksListEl.hidden = true;
    }
  }

  private reconcileCalls(
    sessionEntry: SessionDomEntry,
    calls: ProjectCallView[],
    nowMs: number
  ): void {
    const nextKeys = new Set(calls.map((call) => call.key));

    for (const [key, callEntry] of sessionEntry.callEntries.entries()) {
      if (!nextKeys.has(key)) {
        callEntry.element.remove();
        sessionEntry.callEntries.delete(key);
      }
    }

    sessionEntry.callsContainer.hidden = calls.length === 0;
    const anyRunning = calls.some((call) => call.running);
    sessionEntry.callsContainer.classList.toggle('tp-calls-idle', !anyRunning);
    sessionEntry.callsContainer.setAttribute(
      'aria-label',
      anyRunning ? 'Running agents' : 'Recent calls'
    );

    for (let index = 0; index < calls.length; index += 1) {
      const call = calls[index];
      let callEntry = sessionEntry.callEntries.get(call.key);
      if (!callEntry) {
        callEntry = this.createCallEntry(call);
        sessionEntry.callEntries.set(call.key, callEntry);
      }
      this.updateCallEntry(callEntry, call, nowMs);

      const targetNode = sessionEntry.callsContainer.children[index] ?? null;
      if (targetNode !== callEntry.element) {
        sessionEntry.callsContainer.insertBefore(callEntry.element, targetNode);
      }
    }
  }

  private createCallEntry(call: ProjectCallView): CallDomEntry {
    const iconEl = createBrandIconElement(call.brand);
    iconEl.classList.add('tp-provider');

    const roleTextNode = document.createTextNode(call.role);
    const purposeEl = h('span', { className: 'tp-call-purpose' });
    const nameEl = h('span', { className: 'tp-call-name' }, roleTextNode, purposeEl);
    const modelEl = h('span', { className: 'tp-model' });
    const copyEl = h('div', { className: 'tp-call-copy' }, nameEl, modelEl);

    const spinnerEl = h('span', {
      className: 'tp-spinner',
      role: 'status',
      'aria-label': 'Running',
      hidden: true,
    });
    const durationTextEl = h('span', { className: 'tp-call-duration' });
    const metricFirstRowEl = h('span', undefined, spinnerEl, durationTextEl);
    const usageEl = h('span', { className: 'tp-call-usage' });
    const metricEl = h('span', { className: 'tp-call-metric' }, metricFirstRowEl, usageEl);
    const detailMetaEl = h('span', { className: 'tp-sr-only tp-call-detail', hidden: true });

    const element = h(
      'div',
      { className: 'tp-call', 'data-call-key': call.key },
      iconEl,
      copyEl,
      metricEl,
      detailMetaEl
    );

    return {
      key: call.key,
      call,
      brand: call.brand,
      element,
      iconEl,
      roleTextNode,
      purposeEl,
      modelEl,
      metricFirstRowEl,
      spinnerEl,
      durationTextEl,
      usageEl,
      detailMetaEl,
    };
  }

  private updateCallEntry(entry: CallDomEntry, call: ProjectCallView, _nowMs: number): void {
    entry.call = call;
    if (entry.brand !== call.brand) {
      const nextIcon = createBrandIconElement(call.brand);
      nextIcon.classList.add('tp-provider');
      entry.iconEl.replaceWith(nextIcon);
      entry.iconEl = nextIcon;
      entry.brand = call.brand;
    }

    entry.element.className = call.running ? 'tp-call' : 'tp-call tp-call-finished';
    entry.element.setAttribute('data-running', call.running ? 'true' : 'false');
    entry.detailMetaEl.textContent = call.detail;

    const role = call.role.trim();
    const title = call.title.trim();
    entry.roleTextNode.nodeValue = role || title || 'Agent';
    if (role && title && title.toLowerCase() !== role.toLowerCase()) {
      entry.purposeEl.textContent = ` · ${title}`;
      entry.purposeEl.hidden = false;
    } else {
      entry.purposeEl.textContent = '';
      entry.purposeEl.hidden = true;
    }

    entry.modelEl.textContent = formatCallModelLine(call);
    entry.spinnerEl.hidden = !call.running;

    if (call.running) {
      entry.durationTextEl.textContent = call.durationLabel || 'Running';
    } else {
      entry.durationTextEl.textContent = formatFinishedDurationText(call);
    }

    entry.usageEl.textContent = call.usageLabel;
  }

  private reconcileEvents(
    sessionEntry: SessionDomEntry,
    events: ProjectEventView[],
    nowMs: number
  ): void {
    const visibleEvents = events.slice(0, MAX_RECENT_EVENTS);
    const nextKeys = new Set(visibleEvents.map((event) => event.key));

    for (const [key, eventEntry] of sessionEntry.eventEntries.entries()) {
      if (!nextKeys.has(key)) {
        eventEntry.element.remove();
        sessionEntry.eventEntries.delete(key);
      }
    }

    sessionEntry.recentDetails.hidden = visibleEvents.length === 0;

    for (let index = 0; index < visibleEvents.length; index += 1) {
      const event = visibleEvents[index];
      let eventEntry = sessionEntry.eventEntries.get(event.key);
      if (!eventEntry) {
        eventEntry = this.createEventEntry(event);
        sessionEntry.eventEntries.set(event.key, eventEntry);
      }
      this.updateEventEntry(eventEntry, event, nowMs);

      const targetNode = sessionEntry.eventsList.children[index] ?? null;
      if (targetNode !== eventEntry.element) {
        sessionEntry.eventsList.insertBefore(eventEntry.element, targetNode);
      }
    }
  }

  private createEventEntry(event: ProjectEventView): EventDomEntry {
    const markEl = h('span', { className: 'tp-event-mark' });
    const labelEl = h('span', { className: 'tp-event-label' }, event.label);
    const timeEl = h('time', { className: 'tp-event-time' });
    const detailMetaEl = h('span', { className: 'tp-sr-only tp-event-detail', hidden: true });
    const element = h(
      'li',
      { className: 'tp-event', 'data-event-key': event.key },
      markEl,
      labelEl,
      timeEl,
      detailMetaEl
    );

    return {
      key: event.key,
      event,
      element,
      labelEl,
      timeEl,
      detailMetaEl,
    };
  }

  private updateEventEntry(entry: EventDomEntry, event: ProjectEventView, nowMs: number): void {
    entry.event = event;
    entry.labelEl.textContent = event.label;
    entry.timeEl.textContent = formatRelativeTime(event.atMs, nowMs);
    entry.detailMetaEl.textContent = event.detail;
  }

  private tryApplyFocusScope(scope: ActivityFeedScope): boolean {
    const targetSourceId = typeof scope.source_id === 'string' ? scope.source_id.trim() : '';
    const targetJobId = typeof scope.job_id === 'string' ? scope.job_id.trim() : '';
    if (!targetSourceId || !targetJobId) {
      return false;
    }

    for (const projectEntry of this.projectEntries.values()) {
      for (const sessionEntry of projectEntry.sessionEntries.values()) {
        for (const jobEntry of sessionEntry.jobEntries.values()) {
          const job = jobEntry.jobView.job;
          if (job.source_id === scope.source_id && job.id === scope.job_id) {
            this.clearFocusedJobHighlight();
            this.focusedJobKey = jobEntry.key;

            projectEntry.details.open = true;
            this.userProjectOpenState.set(projectEntry.key, true);

            sessionEntry.details.open = true;
            this.userSessionOpenState.set(sessionEntry.key, true);

            if (
              jobEntry.jobView.olderThanWindow ||
              jobEntry.element.parentElement === sessionEntry.olderJobsListEl
            ) {
              sessionEntry.olderJobsDetails.open = true;
              this.userOlderJobsOpenState.set(sessionEntry.key, true);
            }

            jobEntry.element.open = true;
            getUserJobOpenState(this).set(jobEntry.key, true);

            jobEntry.element.classList.add('tp-job-focused');
            jobEntry.element.setAttribute('data-focused', 'true');
            try {
              jobEntry.element.focus();
            } catch {
              // Ignore focus errors in non-interactive test environments.
            }
            jobEntry.element.scrollIntoView?.({ block: 'nearest' });
            return true;
          }
        }
      }
    }
    return false;
  }

  private clearFocusedJobHighlight(): void {
    for (const projectEntry of this.projectEntries.values()) {
      for (const sessionEntry of projectEntry.sessionEntries.values()) {
        for (const jobEntry of sessionEntry.jobEntries.values()) {
          jobEntry.element.classList.remove('tp-job-focused');
          jobEntry.element.removeAttribute('data-focused');
        }
      }
    }
  }
}
