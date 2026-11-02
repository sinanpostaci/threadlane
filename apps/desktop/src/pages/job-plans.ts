import { createUiIcon } from '../icons.ts';
import { h } from '../ui/dom.ts';
import type {
  ObservationJob,
  ObservationSnapshot,
  ObservationTask,
} from '../observation/types.ts';

import {
  sourceLabel,
  jobPlanKey,
  jobActivityScope,
  buildJobPlanGroups,
  formatJobLifecycle,
  formatRecordedStatus,
  formatReviewState,
  boundConversationLabel,
  summarizeJobProgress,
  summarizeActiveCalls,
  resolveTaskDependencies,
  describeJobPlansState,
  jobPlanRowSignature,
  jobPlanGroupsSignature,
  jobPlanRenderSignature,
} from './job-plans-model.ts';
import type {
  JobPlanProjectGroup,
  JobPlansPageOptions,
  JobPlansUpdateOptions,
} from './job-plans-model.ts';
export { jobPlanKey, jobActivityScope, compareObservationJobs, buildJobPlanGroups, formatJobLifecycle, formatRecordedStatus, formatReviewState, boundConversationLabel, summarizeJobProgress, summarizeActiveCalls, resolveTaskDependencies, describeJobPlansState, jobPlanRowSignature, jobPlanGroupsSignature, jobPlanRenderSignature } from './job-plans-model.ts';
export type { JobPlansPageOptions, JobPlansUpdateOptions, JobPlanProjectGroup, JobPlansStateSummary, ResolvedTaskDependency } from './job-plans-model.ts';

const UNTITLED_TASK = 'Untitled task';
const UNTITLED_JOB = 'Untitled job';
const UNNAMED_SOURCE = 'Unnamed source';
const NOT_RECORDED = 'Not recorded';
const ACROSS_LINKED_PROJECTS = 'Across linked projects';

interface ProjectView {
  element: HTMLElement;
  heading: HTMLElement;
  name: HTMLElement;
  count: HTMLElement;
}

interface JobRowView {
  element: HTMLDetailsElement;
  title: HTMLElement;
  status: HTMLElement;
  chat: HTMLElement;
  progress: HTMLElement | null;
  body: HTMLElement;
  signature: string;
}

function jobCountLabel(count: number): string {
  return `${count} job${count === 1 ? '' : 's'}`;
}

function createChevron(): SVGSVGElement {
  const icon = createUiIcon('chevron-right');
  icon.classList.add('tl-chevron');
  return icon;
}

function taskStatusText(task: ObservationTask): string {
  return formatRecordedStatus(task.status);
}

function buildTaskTechnicalIdentifiers(job: ObservationJob): HTMLElement | null {
  if (job.tasks.length === 0) {
    return null;
  }
  return h(
    'div',
    { className: 'aq-job-technical-tasks' },
    h(
      'p',
      { className: 'aq-job-technical-heading' },
      'Recorded task and dependency IDs'
    ),
    h(
      'ul',
      { className: 'aq-job-technical-task-list' },
      ...job.tasks.map((task) =>
        h(
          'li',
          { className: 'aq-job-technical-task' },
          h(
            'span',
            { className: 'aq-job-technical-task-title' },
            task.title.trim() || UNTITLED_TASK
          ),
          h(
            'dl',
            { className: 'aq-technical-grid' },
            h('dt', undefined, 'Task ID'),
            h('dd', undefined, task.id),
            h('dt', undefined, 'Dependency IDs'),
            h(
              'dd',
              undefined,
              task.depends_on.length > 0 ? task.depends_on.join(', ') : 'None recorded'
            )
          )
        )
      )
    )
  );
}

export class JobPlansPage {
  public readonly element: HTMLElement;
  private readonly onViewActivity?: JobPlansPageOptions['onViewActivity'];
  // Retained for API compatibility; the app chrome owns access to the editor.
  private readonly onOpenAdvanced?: JobPlansPageOptions['onOpenAdvanced'];
  private readonly headCount: HTMLElement;
  private readonly errorEl: HTMLElement;
  private readonly emptyEl: HTMLElement;
  private readonly jobsEl: HTMLElement;
  private readonly noticesEl: HTMLElement;
  private readonly expanded = new Set<string>();
  private readonly technicalExpanded = new Set<string>();
  private readonly projectViews = new Map<string, ProjectView>();
  private readonly jobViews = new Map<string, JobRowView>();
  private snapshot: ObservationSnapshot | null = null;
  private options: JobPlansUpdateOptions = {};
  private requestedFocusKey: string | null = null;
  private lastSignature: string | null = null;
  private lastGroupsSignature: string | null = null;
  private disposed = false;

  constructor(options: JobPlansPageOptions = {}) {
    this.onViewActivity = options.onViewActivity;
    this.onOpenAdvanced = options.onOpenAdvanced;
    this.headCount = h('span', { className: 'tl-muted' }, '0 jobs');
    this.errorEl = h('div', {
      className: 'aq-banner aq-banner-error',
      role: 'alert',
      hidden: true,
    });
    this.emptyEl = h('p', { className: 'tl-empty', hidden: true });
    this.jobsEl = h('div', {
      className: 'tl-jobs',
      'aria-label': 'Independent jobs grouped by project',
    });
    this.noticesEl = h('ul', {
      className: 'tl-notices',
      hidden: true,
    });
    this.element = h(
      'div',
      { className: 'aq-job-plans' },
      h(
        'div',
        { className: 'tl-head' },
        h('h2', undefined, 'Plans'),
        this.headCount
      ),
      h(
        'div',
        { className: 'tl-tools' },
        h('span', { className: 'tl-muted' }, ACROSS_LINKED_PROJECTS)
      ),
      this.errorEl,
      this.emptyEl,
      this.jobsEl,
      this.noticesEl
    );
    this.element.addEventListener('click', (event) => {
      this.handleClick(event);
    });
    this.element.addEventListener(
      'toggle',
      (event) => {
        this.handleToggle(event);
      },
      true
    );
  }

  public update(
    snapshot: ObservationSnapshot | null,
    options: JobPlansUpdateOptions = {}
  ): void {
    if (this.disposed) {
      return;
    }
    this.snapshot = snapshot;
    this.options = {
      loading: options.loading === true,
      error: options.error ?? null,
    };
    this.render();
  }

  /**
   * Expands and focuses one exact source/job pair without relying on chat,
   * binding, title, or the desktop's global selected binding.
   */
  public focusJob(scope: { source_id: string; job_id: string }): void {
    if (this.disposed) {
      return;
    }
    const key = jobPlanKey({ source_id: scope.source_id, id: scope.job_id });
    this.expanded.add(key);
    this.requestedFocusKey = key;
    if (this.jobViews.has(key) && this.applyRequestedFocus()) {
      return;
    }
    this.render(true);
  }

  public dispose(): void {
    this.disposed = true;
    this.expanded.clear();
    this.technicalExpanded.clear();
    this.projectViews.clear();
    this.jobViews.clear();
    this.requestedFocusKey = null;
    this.lastSignature = null;
    this.lastGroupsSignature = null;
    this.element.replaceChildren();
  }

  private handleClick(event: MouseEvent): void {
    const target = event.target as HTMLElement | null;
    const actionEl = target?.closest<HTMLElement>('[data-job-action]');
    if (!actionEl || !this.element.contains(actionEl)) {
      return;
    }
    const action = actionEl.dataset.jobAction;
    if (action === 'view-activity') {
      const sourceId = actionEl.dataset.jobSource;
      const jobId = actionEl.dataset.jobId;
      if (sourceId && jobId) {
        this.onViewActivity?.({ source_id: sourceId, job_id: jobId });
      }
      return;
    }
    if (action === 'open-advanced') {
      this.onOpenAdvanced?.();
    }
  }

  private handleToggle(event: Event): void {
    if (this.disposed) {
      return;
    }
    const target = event.target;
    if (!(target instanceof HTMLElement) || target.tagName !== 'DETAILS') {
      return;
    }
    const details = target as HTMLDetailsElement;
    if (!this.element.contains(details)) {
      return;
    }
    const technicalKey = details.dataset.jobTechnicalKey;
    if (technicalKey) {
      if (details.open) {
        this.technicalExpanded.add(technicalKey);
      } else {
        this.technicalExpanded.delete(technicalKey);
      }
      return;
    }
    const jobKey = details.dataset.jobKey;
    if (jobKey) {
      if (details.open) {
        this.expanded.add(jobKey);
      } else {
        this.expanded.delete(jobKey);
      }
    }
  }

  private findFocusTarget(focusKey: string): HTMLElement | null {
    for (const element of this.element.querySelectorAll<HTMLElement>('[data-job-focus]')) {
      if (element.dataset.jobFocus === focusKey) {
        return element;
      }
    }
    return null;
  }

  private applyRequestedFocus(): boolean {
    const key = this.requestedFocusKey;
    if (!key) {
      return false;
    }
    const target = this.findFocusTarget(`${key}:summary`);
    if (!target) {
      return false;
    }
    this.requestedFocusKey = null;
    const details = target.closest('details.tl-job') as HTMLDetailsElement | null;
    if (details) {
      details.open = true;
      this.expanded.add(key);
    }
    target.focus();
    if (typeof target.scrollIntoView === 'function') {
      target.scrollIntoView({ block: 'nearest' });
    }
    return true;
  }

  private syncExpansion(): void {
    for (const details of this.element.querySelectorAll<HTMLDetailsElement>(
      'details[data-job-key]'
    )) {
      const key = details.dataset.jobKey;
      if (!key) {
        continue;
      }
      if (details.open) {
        this.expanded.add(key);
      } else {
        this.expanded.delete(key);
      }
    }
    for (const details of this.element.querySelectorAll<HTMLDetailsElement>(
      'details[data-job-technical-key]'
    )) {
      const key = details.dataset.jobTechnicalKey;
      if (!key) {
        continue;
      }
      if (details.open) {
        this.technicalExpanded.add(key);
      } else {
        this.technicalExpanded.delete(key);
      }
    }
  }

  private render(force: boolean = false): void {
    if (this.disposed) {
      return;
    }
    const state = describeJobPlansState(this.snapshot, this.options);
    const groups = buildJobPlanGroups(this.snapshot);
    const groupsSignature = jobPlanGroupsSignature(this.snapshot);
    const signature = jobPlanRenderSignature(this.snapshot, this.options);
    if (!force && signature === this.lastSignature) {
      return;
    }
    this.lastSignature = signature;
    const groupsChanged = groupsSignature !== this.lastGroupsSignature;
    this.lastGroupsSignature = groupsSignature;

    const active =
      typeof document !== 'undefined' && document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    const focusKey =
      active && this.element.contains(active) ? active.dataset.jobFocus ?? null : null;
    const scroller = this.element.closest('.aq-main') as HTMLElement | null;
    const scrollTop = scroller?.scrollTop;

    this.syncExpansion();

    const jobCount = groups.reduce((count, group) => count + group.jobs.length, 0);
    this.setHeadCount(jobCount);
    this.renderError(state.error);
    this.renderEmpty(state.error, state.emptyMessage);
    this.renderNotices(state.notices);

    if (force || groupsChanged) {
      const sourceLabels = new Map(
        (this.snapshot?.sources ?? []).map(
          (source) => [source.id, sourceLabel(source)] as const
        )
      );
      this.reconcileProjects(groups, sourceLabels);
    }

    if (this.requestedFocusKey && this.applyRequestedFocus()) {
      return;
    }

    const focusTarget = focusKey ? this.findFocusTarget(focusKey) : null;
    if (focusTarget && !(focusTarget as { disabled?: boolean }).disabled) {
      focusTarget.focus({ preventScroll: true });
    }
    if (scroller && scrollTop !== undefined) {
      scroller.scrollTop = scrollTop;
    }
  }

  private setHeadCount(count: number): void {
    const label = jobCountLabel(count);
    if (this.headCount.textContent !== label) {
      this.headCount.textContent = label;
    }
  }

  private renderError(error: string | null): void {
    if (!error) {
      if (!this.errorEl.hidden) {
        this.errorEl.hidden = true;
      }
      return;
    }
    if (this.errorEl.hidden) {
      this.errorEl.hidden = false;
    }
    if (this.errorEl.textContent !== error) {
      this.errorEl.textContent = error;
    }
  }

  private renderEmpty(error: string | null, emptyMessage: string | null): void {
    const message = error ? null : emptyMessage;
    if (!message) {
      if (!this.emptyEl.hidden) {
        this.emptyEl.hidden = true;
      }
      return;
    }
    if (this.emptyEl.hidden) {
      this.emptyEl.hidden = false;
    }
    if (this.emptyEl.textContent !== message) {
      this.emptyEl.textContent = message;
    }
  }

  private renderNotices(notices: readonly string[]): void {
    if (notices.length === 0) {
      if (!this.noticesEl.hidden) {
        this.noticesEl.hidden = true;
      }
      if (this.noticesEl.childElementCount > 0) {
        this.noticesEl.replaceChildren();
      }
      return;
    }
    if (this.noticesEl.hidden) {
      this.noticesEl.hidden = false;
    }
    const current = Array.from(
      this.noticesEl.children,
      (child) => child.textContent ?? ''
    );
    if (
      current.length === notices.length &&
      current.every((text, index) => text === notices[index])
    ) {
      return;
    }
    this.noticesEl.replaceChildren(
      ...notices.map((notice) => h('li', undefined, notice))
    );
  }

  private reconcileProjects(
    groups: JobPlanProjectGroup[],
    sourceLabels: ReadonlyMap<string, string>
  ): void {
    const seenProjects = new Set<string>();
    const seenJobs = new Set<string>();
    const fragment = document.createDocumentFragment();

    for (const group of groups) {
      seenProjects.add(group.key);
      let project = this.projectViews.get(group.key);
      if (!project) {
        project = this.buildProjectView(group);
        this.projectViews.set(group.key, project);
      }
      this.updateProjectView(project, group);

      const desired = new Set<HTMLElement>([project.heading]);
      for (const job of group.jobs) {
        const key = jobPlanKey(job);
        seenJobs.add(key);
        const sourceLabelValue = sourceLabels.get(job.source_id) ?? UNNAMED_SOURCE;
        const signature = jobPlanRowSignature(job, sourceLabelValue);
        let row = this.jobViews.get(key);
        if (!row) {
          row = this.buildJobRowView(job, sourceLabelValue);
          row.signature = signature;
          this.jobViews.set(key, row);
        } else if (row.signature !== signature) {
          this.updateJobRowView(row, job, sourceLabelValue);
          row.signature = signature;
        }
        project.element.appendChild(row.element);
        desired.add(row.element);
      }

      for (const child of Array.from(project.element.children)) {
        if (!desired.has(child as HTMLElement)) {
          child.remove();
        }
      }
      fragment.appendChild(project.element);
    }

    this.jobsEl.replaceChildren(fragment);

    for (const [key, project] of this.projectViews) {
      if (!seenProjects.has(key)) {
        project.element.remove();
        this.projectViews.delete(key);
      }
    }
    for (const [key, row] of this.jobViews) {
      if (!seenJobs.has(key)) {
        row.element.remove();
        this.jobViews.delete(key);
      }
    }
  }

  private buildProjectView(group: JobPlanProjectGroup): ProjectView {
    const name = h('h3', undefined, group.label);
    const count = h(
      'span',
      { className: 'tl-muted' },
      jobCountLabel(group.jobs.length)
    );
    const heading = h(
      'div',
      { className: 'tl-project-heading' },
      name,
      count
    );
    const element = h(
      'section',
      {
        className: 'tl-project',
        'aria-label': `${group.label} jobs`,
      },
      heading
    );
    return { element, heading, name, count };
  }

  private updateProjectView(project: ProjectView, group: JobPlanProjectGroup): void {
    if (project.name.textContent !== group.label) {
      project.name.textContent = group.label;
    }
    const count = jobCountLabel(group.jobs.length);
    if (project.count.textContent !== count) {
      project.count.textContent = count;
    }
    const ariaLabel = `${group.label} jobs`;
    if (project.element.getAttribute('aria-label') !== ariaLabel) {
      project.element.setAttribute('aria-label', ariaLabel);
    }
  }

  private buildJobRowView(
    job: ObservationJob,
    sourceLabelValue: string
  ): JobRowView {
    const key = jobPlanKey(job);
    const title = h('span', undefined, '');
    const status = h('span', { className: 'tl-job-status' }, '');
    const chat = h('div', { className: 'tl-job-context' }, '');
    const body = h('div', { className: 'tl-job-body' });
    const element = h(
      'details',
      {
        className: 'tl-job',
        'data-job-key': key,
        open: this.expanded.has(key),
      },
      h(
        'summary',
        { 'data-job-focus': `${key}:summary` },
        h(
          'div',
          { className: 'tl-job-title' },
          title,
          status,
          createChevron()
        ),
        chat
      ),
      body
    );
    const view: JobRowView = {
      element,
      title,
      status,
      chat,
      progress: null,
      body,
      signature: '',
    };
    this.updateJobRowView(view, job, sourceLabelValue);
    return view;
  }

  private updateJobRowView(
    view: JobRowView,
    job: ObservationJob,
    sourceLabelValue: string
  ): void {
    const key = jobPlanKey(job);
    const title = job.title.trim() || UNTITLED_JOB;
    const lifecycle = formatJobLifecycle(job.state);
    const active = job.state === 'running' || job.state === 'reviewing';
    const progress = summarizeJobProgress(job);
    const activeCalls = summarizeActiveCalls(job);
    const progressParts = [progress, activeCalls].filter(
      (value): value is string => Boolean(value)
    );

    if (view.title.textContent !== title) {
      view.title.textContent = title;
    }
    const statusClass = active
      ? 'tl-job-status tl-job-status-active'
      : 'tl-job-status';
    if (view.status.className !== statusClass) {
      view.status.className = statusClass;
    }
    if (view.status.textContent !== lifecycle) {
      view.status.textContent = lifecycle;
    }
    const chat = `Chat · ${boundConversationLabel(job)}`;
    if (view.chat.textContent !== chat) {
      view.chat.textContent = chat;
    }
    const progressText = progressParts.join(' · ');
    if (!progressText) {
      view.progress?.remove();
      view.progress = null;
    } else if (!view.progress) {
      const progressEl = h(
        'div',
        { className: 'tl-job-context' },
        progressText
      );
      view.progress = progressEl;
      view.chat.after(progressEl);
    } else if (view.progress.textContent !== progressText) {
      view.progress.textContent = progressText;
    }

    view.body.replaceChildren(
      ...this.buildJobBody(job, sourceLabelValue, key)
    );
  }

  private buildJobBody(
    job: ObservationJob,
    sourceLabelValue: string,
    key: string
  ): Node[] {
    const scope = jobActivityScope(job);
    const summary = job.summary?.trim() || NOT_RECORDED;
    const outcome = h(
      'div',
      undefined,
      h('div', { className: 'tl-label' }, 'Agreed outcome'),
      h('p', undefined, summary)
    );
    const steps = this.buildTaskSteps(job);
    const approval = h(
      'div',
      undefined,
      h('div', { className: 'tl-label' }, 'Approval & execution'),
      h(
        'p',
        { className: 'tl-detail' },
        `${formatRecordedStatus(job.approval_status)} · ${formatRecordedStatus(
          job.execution_status
        )} · Review: ${formatReviewState(job.review_state)}`
      )
    );
    const action = h(
      'button',
      {
        type: 'button',
        className: 'tl-action tl-job-link',
        'data-job-action': 'view-activity',
        'data-job-source': scope.source_id,
        'data-job-id': scope.job_id,
        'data-job-focus': `${key}:activity`,
      },
      'View activity'
    );
    const technical = this.buildTechnicalIdentifiers(job, sourceLabelValue, key);
    return [outcome, steps, approval, action, technical];
  }

  private buildTaskSteps(job: ObservationJob): HTMLElement {
    const section = h(
      'div',
      undefined,
      h('div', { className: 'tl-label' }, 'Steps')
    );
    if (job.tasks.length === 0) {
      section.appendChild(h('p', { className: 'tl-detail' }, 'No steps recorded'));
      return section;
    }
    for (const [index, task] of job.tasks.entries()) {
      const taskEl = h(
        'div',
        { className: 'tl-task' },
        h(
          'span',
          undefined,
          `${index + 1}. ${task.title.trim() || UNTITLED_TASK}`
        ),
        h('span', { className: 'tl-detail' }, taskStatusText(task))
      );
      const dependencies = resolveTaskDependencies(job, task);
      if (dependencies.length > 0) {
        taskEl.appendChild(
          h(
            'span',
            { className: 'tl-detail' },
            `Depends on: ${dependencies
              .map((dependency) => dependency.title ?? 'Unresolved dependency')
              .join('; ')}`
          )
        );
      }
      section.appendChild(taskEl);
    }
    return section;
  }

  private buildTechnicalIdentifiers(
    job: ObservationJob,
    sourceLabelValue: string,
    key: string
  ): HTMLDetailsElement {
    return h(
      'details',
      {
        className: 'tl-technical',
        'data-job-technical-key': key,
        open: this.technicalExpanded.has(key),
      },
      h(
        'summary',
        { 'data-job-focus': `${key}:technical` },
        h('span', undefined, 'Technical identifiers'),
        createChevron()
      ),
      h(
        'dl',
        { className: 'aq-technical-grid' },
        h('dt', undefined, 'Job ID'),
        h('dd', undefined, job.id),
        h('dt', undefined, 'Source ID'),
        h('dd', undefined, job.source_id),
        h('dt', undefined, 'Source'),
        h('dd', undefined, sourceLabelValue),
        h('dt', undefined, 'Project path'),
        h('dd', undefined, job.project_path ?? NOT_RECORDED),
        h('dt', undefined, 'Plan ID'),
        h('dd', undefined, job.plan_id ?? NOT_RECORDED),
        h('dt', undefined, 'Plan revision'),
        h(
          'dd',
          undefined,
          job.plan_revision !== null ? String(job.plan_revision) : NOT_RECORDED
        ),
        h('dt', undefined, 'Thread UUID'),
        h('dd', undefined, job.thread_uuid ?? NOT_RECORDED),
        h('dt', undefined, 'Binding ID'),
        h('dd', undefined, job.binding_id ?? NOT_RECORDED)
      ),
      buildTaskTechnicalIdentifiers(job)
    );
  }
}
