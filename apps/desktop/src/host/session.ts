import { createBrandIconElement, createUiIcon } from '../icons.ts';
import { hasTauriRuntime, isDevPreviewFixtureMode } from '../registry/ipc.ts';
import { createButton, h } from '../ui/dom.ts';
import type { ApprovalSummary } from '../approval/types.ts';
import type { BrandKey } from '../types.ts';
import {
  desktop_approved_session,
  desktop_approved_task,
  desktop_start_approved_session,
} from './ipc.ts';
import {
  approvalContextMatches,
  buildHostRequestKey,
  canStartLocalSession,
  classifyHostError,
  compactDisplayText,
  formatAdapterLabel,
  formatHostStatusLabel,
  formatHostStatusTone,
  formatTaskInstruction,
  formatTaskProgress,
  formatTaskStatusLabel,
  formatTaskStatusTone,
  formatTaskTelemetry,
  HOST_STATUS_POLL_MS,
  hostSummaryMatchesContext,
  hostTaskMatchesContext,
  isCurrentHostRequest,
  isHostSummaryReady,
  isTerminalTaskStatus,
  MAX_VISIBLE_APPROVED_TASKS,
  nextVisibleApprovedTaskCount,
  normalizeApprovedTaskIds,
  resolveHostTaskBrand,
  taskReviewNote,
} from './session-logic.ts';
import type { HostErrorKind } from './session-logic.ts';
import type {
  HostPanelContext,
  HostSessionSummary,
  TaskStateSnapshot,
  WorkerAcceptanceState,
} from './types.ts';

export type { HostPanelContext } from './types.ts';

if (typeof window !== 'undefined' && typeof import.meta.glob === 'function') {
  import.meta.glob('./session.css', { eager: true });
}

const MAX_CONCURRENT_TASK_READS = 4;
const INSTRUCTION_DISPLAY_LIMIT = 1_200;
const PROGRESS_DISPLAY_LIMIT = 600;
const OUTCOME_DISPLAY_LIMIT = 700;

type StatusReadReason = 'open' | 'explicit' | 'poll' | 'context';
type TaskReadMode = 'missing' | 'active' | 'all';

interface TaskRowView {
  root: HTMLElement;
  brandHost: HTMLElement;
  brand: BrandKey | null;
  badge: HTMLElement;
  meta: HTMLElement;
  requested: HTMLElement;
  observed: HTMLElement;
  progress: HTMLElement;
  instruction: HTMLElement;
  outcome: HTMLElement;
  review: HTMLElement;
}

interface TaskReadRequest {
  readKey: string;
  taskId: string;
  epoch: number;
  key: string;
  sessionBindingId: string | null;
  approvalId: string;
}

function approvalStateKey(approval: ApprovalSummary): string {
  return JSON.stringify([
    approval.approval_id,
    approval.subject_digest,
    approval.plan_id,
    approval.plan_revision,
    approval.binding_id,
    approval.thread_uuid,
    approval.approved_task_ids,
  ]);
}

function panelContextKey(context: HostPanelContext): string {
  return JSON.stringify([context.bindingId, context.threadUuid, context.connected]);
}

function kvRow(label: string, value: string): HTMLElement {
  return h(
    'div',
    { className: 'aq-host-session-kv-row' },
    h('dt', undefined, label),
    h('dd', undefined, value)
  );
}

function setText(element: HTMLElement, text: string): void {
  if (element.textContent !== text) {
    element.textContent = text;
  }
}

function formatAcceptanceState(state: WorkerAcceptanceState): string {
  switch (state) {
    case 'pending_review':
      return 'Pending review';
    case 'ready_for_review':
      return 'Ready for review';
    case 'rejected_by_diff_gate':
      return 'Rejected by diff gate';
    case 'execution_failed':
      return 'Execution failed';
    case 'unknown':
      return 'Unknown (uncertain)';
    default:
      return 'Unknown (uncertain)';
  }
}

function formatTaskOutcome(task: TaskStateSnapshot): string {
  const outcome = task.worker?.outcome;
  if (!outcome) {
    return '';
  }
  const parts: string[] = [];
  const finalSummary = outcome.final_summary?.trim();
  const failureReason = outcome.failure_reason?.trim();
  if (finalSummary) {
    parts.push(finalSummary);
  }
  if (failureReason) {
    parts.push(`Failure: ${failureReason}`);
  }
  parts.push(`Review state: ${formatAcceptanceState(outcome.acceptance_state)}`);
  return compactDisplayText(parts.join(' · '), OUTCOME_DISPLAY_LIMIT);
}

/**
 * Quiet, read-only local-session status for one frozen operator approval.
 *
 * The panel owns only its host node. It never renders the surrounding Plans
 * form, never starts a task or model, and keeps all approval/worker data in
 * memory only while expanded.
 */
export class ApprovedSessionPanel {
  private readonly root: HTMLDetailsElement;
  private readonly summaryBadge: HTMLElement;
  private readonly notice: HTMLElement;
  private readonly statusBadge: HTMLElement;
  private readonly statusMeta: HTMLElement;
  private readonly refreshButton: HTMLButtonElement;
  private readonly startButton: HTMLButtonElement;
  private readonly startButtonLabel: HTMLElement;
  private readonly actions: HTMLElement;
  private readonly techSection: HTMLDetailsElement;
  private readonly techBody: HTMLElement;
  private readonly tasksSection: HTMLElement;
  private readonly tasksCount: HTMLElement;
  private readonly tasksHint: HTMLElement;
  private readonly tasksList: HTMLElement;
  private readonly moreButton: HTMLButtonElement;

  private readonly taskRowViews = new Map<string, TaskRowView>();
  private readonly taskSnapshots = new Map<string, TaskStateSnapshot>();
  private readonly taskErrors = new Map<string, string>();
  private readonly taskPending = new Set<string>();
  private readonly taskInFlight = new Map<string, TaskReadRequest>();
  private taskQueue: string[] = [];

  private approval: ApprovalSummary | null = null;
  private context: HostPanelContext | null = null;
  private approvalKey = '';
  private contextKey = '';
  private summary: HostSessionSummary | null = null;
  private errorMessage: string | null = null;
  private lastErrorKind: HostErrorKind | null = null;
  private statusLoading = false;
  private statusReason: StatusReadReason = 'open';
  private startInFlight = false;
  private startRequestKey: string | null = null;
  private contextEpoch = 0;
  private statusTicket = 0;
  private startTicket = 0;
  private visibleTaskCount = MAX_VISIBLE_APPROVED_TASKS;
  private pollTimer: number | null = null;
  private disposed = false;

  private readonly handleToggle = (): void => {
    if (this.disposed) {
      return;
    }
    if (this.root.open) {
      this.handleOpened();
    } else {
      this.handleClosed();
    }
  };

  private readonly handleVisibilityChange = (): void => {
    if (this.disposed) {
      return;
    }
    if (document.visibilityState === 'hidden') {
      this.stopPolling();
      return;
    }
    if (this.root.open) {
      this.ensurePolling();
    }
  };

  constructor() {
    const summaryBadge = h(
      'span',
      { className: 'aq-host-session-badge', 'data-status': 'default' },
      'Not checked'
    );
    const detailsSummary = h(
      'summary',
      { className: 'aq-host-session-summary' },
      h(
        'span',
        { className: 'aq-host-session-summary-main' },
        createUiIcon('terminal'),
        h('span', { className: 'aq-host-session-title' }, 'Local session')
      ),
      summaryBadge
    );

    const notice = h('p', { className: 'aq-host-session-notice', hidden: true });
    const statusBadge = h(
      'span',
      { className: 'aq-host-session-status-badge', 'data-status': 'default' },
      'Not started'
    );
    const statusMeta = h('p', { className: 'aq-host-session-status-meta' });
    const statusRow = h(
      'div',
      { className: 'aq-host-session-status', 'aria-live': 'polite' },
      h('span', { className: 'aq-host-session-status-label' }, 'Status'),
      statusBadge,
      statusMeta
    );

    const refreshButton = createButton('Refresh status', () => {
      void this.refreshStatus('explicit');
    });
    refreshButton.prepend(createUiIcon('refresh-cw'));
    refreshButton.setAttribute('aria-label', 'Refresh approved local session status');
    refreshButton.hidden = true;

    const startButtonLabel = h('span', undefined, 'Start local session');
    const startButton = h(
      'button',
      { type: 'button', className: 'aq-btn aq-btn-primary' },
      createUiIcon('play'),
      startButtonLabel
    );
    startButton.hidden = true;
    startButton.addEventListener('click', () => {
      void this.handleStart();
    });

    const actions = h(
      'div',
      { className: 'aq-host-session-actions' },
      refreshButton,
      startButton
    );

    const techBody = h('div', { className: 'aq-host-session-tech-body' });
    const techSection = h(
      'details',
      { className: 'aq-host-session-tech' },
      h('summary', undefined, 'Session details'),
      techBody
    );
    techSection.hidden = true;

    const tasksCount = h('span', { className: 'aq-host-session-tasks-count' }, '0 approved');
    const tasksHint = h('p', { className: 'aq-host-session-tasks-hint' });
    const tasksList = h('div', { className: 'aq-host-session-task-list' });
    const moreButton = createButton('Show more', () => {
      this.showMoreTasks();
    });
    moreButton.hidden = true;
    const tasksSection = h(
      'section',
      { className: 'aq-host-session-tasks', 'aria-label': 'Approved task status' },
      h(
        'div',
        { className: 'aq-host-session-tasks-head' },
        h('span', { className: 'aq-host-session-tasks-title' }, 'Approved tasks'),
        tasksCount
      ),
      tasksHint,
      tasksList,
      moreButton
    );

    const body = h(
      'div',
      { className: 'aq-host-session-body' },
      notice,
      statusRow,
      actions,
      techSection,
      tasksSection
    );

    this.root = h('details', { className: 'aq-host-session' }, detailsSummary, body);
    this.summaryBadge = summaryBadge;
    this.notice = notice;
    this.statusBadge = statusBadge;
    this.statusMeta = statusMeta;
    this.refreshButton = refreshButton;
    this.startButton = startButton;
    this.startButtonLabel = startButtonLabel;
    this.actions = actions;
    this.techSection = techSection;
    this.techBody = techBody;
    this.tasksSection = tasksSection;
    this.tasksCount = tasksCount;
    this.tasksHint = tasksHint;
    this.tasksList = tasksList;
    this.moreButton = moreButton;

    this.root.addEventListener('toggle', this.handleToggle);
    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', this.handleVisibilityChange);
    }
    this.syncUi();
  }

  public render(approval: ApprovalSummary, context: HostPanelContext): HTMLElement {
    if (this.disposed) {
      return this.root;
    }

    const nextApprovalKey = approvalStateKey(approval);
    const nextContextKey = panelContextKey(context);
    const changed =
      this.approval === null ||
      nextApprovalKey !== this.approvalKey ||
      nextContextKey !== this.contextKey;

    this.approval = approval;
    this.context = context;

    if (changed) {
      const wasOpen = this.root.open;
      this.approvalKey = nextApprovalKey;
      this.contextKey = nextContextKey;
      this.invalidateState();
      this.syncUi();
      if (wasOpen && this.root.isConnected) {
        this.ensurePolling();
        if (this.canReadStatus()) {
          void this.refreshStatus('context');
        }
      }
    } else {
      this.syncUi();
    }

    return this.root;
  }

  public dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.contextEpoch += 1;
    this.statusTicket += 1;
    this.startTicket += 1;
    this.stopPolling();
    this.taskQueue = [];
    this.taskPending.clear();
    this.taskSnapshots.clear();
    this.taskErrors.clear();
    this.taskInFlight.clear();
    this.taskRowViews.clear();
    this.tasksList.replaceChildren();
    this.summary = null;
    this.errorMessage = null;
    this.lastErrorKind = null;
    this.approval = null;
    this.context = null;
    this.statusLoading = false;
    this.startInFlight = false;
    this.startRequestKey = null;
    this.root.removeEventListener('toggle', this.handleToggle);
    if (typeof document !== 'undefined') {
      document.removeEventListener('visibilitychange', this.handleVisibilityChange);
    }
    this.root.replaceChildren();
  }

  private invalidateState(): void {
    this.stopPolling();
    this.contextEpoch += 1;
    this.statusTicket += 1;
    this.statusLoading = false;
    this.statusReason = 'context';
    this.summary = null;
    this.errorMessage = null;
    this.lastErrorKind = null;
    this.visibleTaskCount = MAX_VISIBLE_APPROVED_TASKS;
    this.clearTaskData();
  }

  private handleOpened(): void {
    this.syncUi();
    this.ensurePolling();
    if (this.root.isConnected && this.canReadStatus()) {
      void this.refreshStatus('open');
    }
  }

  private handleClosed(): void {
    this.stopPolling();
    this.contextEpoch += 1;
    this.statusTicket += 1;
    this.statusLoading = false;
    this.summary = null;
    this.errorMessage = null;
    this.lastErrorKind = null;
    this.visibleTaskCount = MAX_VISIBLE_APPROVED_TASKS;
    this.clearTaskData();
    this.syncUi();
  }

  private clearTaskData(): void {
    this.clearTaskSnapshots();
    this.taskRowViews.clear();
    this.tasksList.replaceChildren();
  }

  private clearTaskSnapshots(): void {
    this.taskSnapshots.clear();
    this.taskErrors.clear();
    this.taskPending.clear();
    this.taskQueue = [];
  }

  private canReadStatus(): boolean {
    return (
      this.approval !== null &&
      this.context !== null &&
      hasTauriRuntime() &&
      approvalContextMatches(this.approval, this.context)
    );
  }

  private currentRequestKey(): string {
    if (!this.approval || !this.context) {
      return '';
    }
    return buildHostRequestKey(this.approval, this.context);
  }

  private isRequestCurrent(epoch: number, key: string): boolean {
    return (
      !this.disposed &&
      this.contextEpoch === epoch &&
      this.currentRequestKey() === key
    );
  }

  private isStatusTicketCurrent(ticket: number, epoch: number, key: string): boolean {
    return (
      this.statusTicket === ticket &&
      isCurrentHostRequest({ epoch, key }, this.contextEpoch, this.currentRequestKey())
    );
  }

  private isStartTicketCurrent(ticket: number, epoch: number, key: string): boolean {
    return (
      this.startTicket === ticket &&
      isCurrentHostRequest({ epoch, key }, this.contextEpoch, this.currentRequestKey())
    );
  }

  private currentReadySession(): HostSessionSummary | null {
    if (!this.summary || !this.approval || !this.context || !hasTauriRuntime()) {
      return null;
    }
    if (!hostSummaryMatchesContext(this.summary, this.approval, this.context)) {
      return null;
    }
    return isHostSummaryReady(this.summary) ? this.summary : null;
  }

  private async refreshStatus(reason: StatusReadReason): Promise<void> {
    if (this.disposed || !this.root.open || this.statusLoading || this.startInFlight) {
      return;
    }
    const approval = this.approval;
    const context = this.context;
    if (!approval || !context || !this.canReadStatus()) {
      this.syncUi();
      return;
    }

    const ticket = ++this.statusTicket;
    const epoch = this.contextEpoch;
    const key = this.currentRequestKey();
    this.statusLoading = true;
    this.statusReason = reason;
    if (reason !== 'poll') {
      this.errorMessage = null;
      this.lastErrorKind = null;
    }
    this.syncUi();

    try {
      const summary = await desktop_approved_session({
        approvalId: approval.approval_id,
      });
      if (!this.isStatusTicketCurrent(ticket, epoch, key)) {
        return;
      }
      if (!hostSummaryMatchesContext(summary, approval, context)) {
        this.summary = null;
        this.errorMessage = 'Session status did not match this approval context.';
        this.lastErrorKind = 'other';
        this.clearTaskSnapshots();
        return;
      }

      const previousSessionBindingId = this.summary?.session_binding_id ?? null;
      this.summary = summary;
      this.errorMessage = null;
      this.lastErrorKind = null;
      if (previousSessionBindingId !== summary.session_binding_id) {
        this.clearTaskSnapshots();
      }

      if (isHostSummaryReady(summary)) {
        this.queueVisibleTaskReads(
          reason === 'explicit' ? 'all' : reason === 'poll' ? 'active' : 'missing'
        );
      } else {
        this.clearTaskSnapshots();
      }
    } catch (err) {
      if (!this.isStatusTicketCurrent(ticket, epoch, key)) {
        return;
      }
      const state = classifyHostError(err);
      this.summary = null;
      this.errorMessage = state.kind === 'not_started' ? null : state.message;
      this.lastErrorKind = state.kind === 'not_started' ? null : state.kind;
      this.clearTaskSnapshots();
    } finally {
      if (this.isStatusTicketCurrent(ticket, epoch, key)) {
        this.statusLoading = false;
        this.syncUi();
      }
    }
  }

  private async handleStart(): Promise<void> {
    if (this.disposed || !this.root.open || this.startInFlight) {
      return;
    }
    const approval = this.approval;
    const context = this.context;
    if (!approval || !context) {
      return;
    }
    const nativeBridgeAvailable = hasTauriRuntime();
    if (
      !canStartLocalSession(approval, context, {
        nativeBridgeAvailable,
        busy: this.statusLoading || this.lastErrorKind === 'bridge_disconnected',
        summary: this.summary,
      })
    ) {
      this.syncUi();
      return;
    }

    // Set the guard synchronously so a second click cannot issue another command.
    this.startInFlight = true;
    const ticket = ++this.startTicket;
    const epoch = this.contextEpoch;
    const key = this.currentRequestKey();
    this.startRequestKey = key;
    this.errorMessage = null;
    this.lastErrorKind = null;
    this.syncUi();

    try {
      const summary = await desktop_start_approved_session({
        approvalId: approval.approval_id,
      });
      if (!this.isStartTicketCurrent(ticket, epoch, key) || !this.root.open) {
        return;
      }
      if (!hostSummaryMatchesContext(summary, approval, context)) {
        this.summary = null;
        this.errorMessage = 'Session result did not match this approval context.';
        this.lastErrorKind = 'other';
        this.clearTaskData();
        return;
      }

      const previousSessionBindingId = this.summary?.session_binding_id ?? null;
      this.summary = summary;
      this.errorMessage = null;
      this.lastErrorKind = null;
      if (previousSessionBindingId !== summary.session_binding_id) {
        this.clearTaskSnapshots();
      }
      if (isHostSummaryReady(summary)) {
        this.queueVisibleTaskReads('missing');
      } else {
        this.clearTaskSnapshots();
      }
    } catch (err) {
      if (!this.isStartTicketCurrent(ticket, epoch, key)) {
        return;
      }
      const state = classifyHostError(err);
      this.summary = null;
      this.errorMessage =
        state.message ?? 'The approved local session could not be started.';
      this.lastErrorKind = state.kind;
      this.clearTaskSnapshots();
    } finally {
      if (this.startTicket === ticket) {
        this.startInFlight = false;
        this.startRequestKey = null;
      }
      if (!this.disposed) {
        if (this.root.open) {
          this.ensurePolling();
        }
        this.syncUi();
      }
    }
  }

  private ensurePolling(): void {
    if (
      this.disposed ||
      this.pollTimer !== null ||
      !this.root.open ||
      !this.canReadStatus() ||
      typeof window === 'undefined'
    ) {
      return;
    }
    this.pollTimer = window.setInterval(() => {
      this.pollTick();
    }, HOST_STATUS_POLL_MS);
  }

  private stopPolling(): void {
    if (this.pollTimer !== null && typeof window !== 'undefined') {
      window.clearInterval(this.pollTimer);
    }
    this.pollTimer = null;
  }

  private pollTick(): void {
    if (this.disposed || !this.root.open) {
      this.stopPolling();
      return;
    }
    if (!this.root.isConnected) {
      return;
    }
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') {
      return;
    }
    if (!this.canReadStatus() || this.statusLoading || this.startInFlight) {
      return;
    }
    void this.refreshStatus('poll');
  }

  private taskReadKeyFor(taskId: string): string {
    const session = this.currentReadySession();
    if (!session) {
      return '';
    }
    return JSON.stringify([
      this.contextEpoch,
      this.currentRequestKey(),
      session.session_binding_id,
      taskId,
    ]);
  }

  private isTaskReadCurrent(request: TaskReadRequest): boolean {
    if (this.disposed || !this.root.open) {
      return false;
    }
    if (!this.isRequestCurrent(request.epoch, request.key)) {
      return false;
    }
    const session = this.currentReadySession();
    return session !== null && session.session_binding_id === request.sessionBindingId;
  }

  private queueVisibleTaskReads(mode: TaskReadMode): void {
    const approval = this.approval;
    if (!approval || !this.currentReadySession()) {
      return;
    }
    const taskIds = normalizeApprovedTaskIds(approval.approved_task_ids).slice(
      0,
      this.visibleTaskCount
    );
    for (const taskId of taskIds) {
      const snapshot = this.taskSnapshots.get(taskId);
      const shouldRead =
        mode === 'all' ||
        !snapshot ||
        (mode === 'active' && !isTerminalTaskStatus(snapshot.status));
      if (!shouldRead || this.taskPending.has(taskId)) {
        continue;
      }
      const readKey = this.taskReadKeyFor(taskId);
      if (!readKey || this.taskInFlight.has(readKey)) {
        continue;
      }
      this.taskQueue.push(taskId);
      this.taskPending.add(taskId);
    }
    this.pumpTaskQueue();
  }

  private pumpTaskQueue(): void {
    if (
      this.disposed ||
      !this.root.open ||
      !this.root.isConnected ||
      (typeof document !== 'undefined' && document.visibilityState === 'hidden')
    ) {
      this.taskQueue = [];
      this.taskPending.clear();
      return;
    }
    if (!this.currentReadySession()) {
      this.taskQueue = [];
      this.taskPending.clear();
      this.syncTaskRows();
      return;
    }

    while (
      this.taskInFlight.size < MAX_CONCURRENT_TASK_READS &&
      this.taskQueue.length > 0
    ) {
      const taskId = this.taskQueue.shift()!;
      const approval = this.approval;
      const context = this.context;
      const session = this.currentReadySession();
      if (!approval || !context || !session) {
        this.taskPending.delete(taskId);
        continue;
      }
      const epoch = this.contextEpoch;
      const key = this.currentRequestKey();
      const readKey = this.taskReadKeyFor(taskId);
      if (!readKey || this.taskInFlight.has(readKey)) {
        this.taskPending.delete(taskId);
        continue;
      }
      const request: TaskReadRequest = {
        readKey,
        taskId,
        epoch,
        key,
        sessionBindingId: session.session_binding_id,
        approvalId: approval.approval_id,
      };
      this.taskInFlight.set(readKey, request);
      void this.performTaskRead(request, approval, context);
    }

    this.syncTaskRows();
  }

  private async performTaskRead(
    request: TaskReadRequest,
    approval: ApprovalSummary,
    context: HostPanelContext
  ): Promise<void> {
    try {
      const snapshot = await desktop_approved_task({
        approvalId: request.approvalId,
        taskId: request.taskId,
      });
      if (!this.isTaskReadCurrent(request)) {
        return;
      }
      if (
        snapshot.task_id !== request.taskId ||
        snapshot.session_binding_id !== request.sessionBindingId ||
        !hostTaskMatchesContext(snapshot, approval, context)
      ) {
        this.taskSnapshots.delete(request.taskId);
        this.taskErrors.set(
          request.taskId,
          'Task status did not match this approval context.'
        );
        return;
      }
      this.taskSnapshots.set(request.taskId, snapshot);
      this.taskErrors.delete(request.taskId);
    } catch (err) {
      if (!this.isTaskReadCurrent(request)) {
        return;
      }
      const state = classifyHostError(err);
      if (state.kind === 'other' || state.kind === 'invalid_input') {
        this.taskSnapshots.delete(request.taskId);
        this.taskErrors.set(
          request.taskId,
          state.message ?? 'Task status is unavailable right now.'
        );
      } else {
        this.summary = null;
        this.errorMessage = state.message;
        this.lastErrorKind = state.kind;
        this.clearTaskSnapshots();
      }
    } finally {
      this.taskInFlight.delete(request.readKey);
      if (this.isTaskReadCurrent(request)) {
        this.taskPending.delete(request.taskId);
      }
      this.pumpTaskQueue();
      if (!this.disposed && this.root.open) {
        this.syncUi();
      }
    }
  }

  private showMoreTasks(): void {
    const approval = this.approval;
    if (!approval) {
      return;
    }
    const total = normalizeApprovedTaskIds(approval.approved_task_ids).length;
    this.visibleTaskCount = nextVisibleApprovedTaskCount(this.visibleTaskCount, total);
    this.syncUi();
    this.queueVisibleTaskReads('missing');
  }

  private createTaskRowView(taskId: string): TaskRowView {
    const brandHost = h('span', { className: 'aq-host-session-task-brand' });
    const badge = h(
      'span',
      { className: 'aq-host-session-task-status', 'data-status': 'default' },
      'Waiting'
    );
    const meta = h('p', { className: 'aq-host-session-task-meta' });
    const requested = h('p', { className: 'aq-host-session-task-text' });
    const observed = h('p', { className: 'aq-host-session-task-text' });
    const progress = h('p', { className: 'aq-host-session-task-text' });
    const instruction = h('p', { className: 'aq-host-session-task-text' });
    const outcome = h('p', { className: 'aq-host-session-task-text' });
    const review = h('p', { className: 'aq-host-session-task-review', hidden: true });

    const detailRow = (label: string, value: HTMLElement): HTMLElement =>
      h(
        'div',
        { className: 'aq-host-session-task-detail-row' },
        h('span', { className: 'aq-host-session-task-detail-label' }, label),
        value
      );

    const root = h(
      'div',
      { className: 'aq-host-session-task', 'data-task-id': taskId },
      h(
        'div',
        { className: 'aq-host-session-task-head' },
        h(
          'span',
          { className: 'aq-host-session-task-name' },
          brandHost,
          h('span', { className: 'aq-host-session-task-id' }, taskId)
        ),
        badge
      ),
      meta,
      h(
        'details',
        { className: 'aq-host-session-task-details' },
        h('summary', undefined, 'Details'),
        h(
          'div',
          { className: 'aq-host-session-task-detail' },
          detailRow('Requested model / effort', requested),
          detailRow('Observed model / effort', observed),
          detailRow('Last progress', progress),
          detailRow('Instruction', instruction),
          detailRow('Outcome', outcome),
          review
        )
      )
    );

    return {
      root,
      brandHost,
      brand: null,
      badge,
      meta,
      requested,
      observed,
      progress,
      instruction,
      outcome,
      review,
    };
  }

  private syncTaskRow(
    view: TaskRowView,
    taskId: string,
    snapshot: TaskStateSnapshot | null,
    pending: boolean,
    error: string | null
  ): void {
    let statusText = 'Waiting';
    let statusTone: 'default' | 'active' | 'warn' = 'default';

    if (snapshot) {
      statusText = formatTaskStatusLabel(snapshot.status);
      statusTone = formatTaskStatusTone(snapshot.status);
    } else if (error) {
      statusText = 'Status unavailable';
      statusTone = 'warn';
    } else if (pending) {
      statusText = 'Reading…';
      statusTone = 'active';
    } else if (!hasTauriRuntime()) {
      statusText = 'Native bridge required';
    } else if (
      !this.approval ||
      !this.context ||
      !approvalContextMatches(this.approval, this.context)
    ) {
      statusText = 'Context mismatch';
      statusTone = 'warn';
    } else if (!this.currentReadySession()) {
      const hostSummary =
        this.summary &&
        this.approval &&
        this.context &&
        hostSummaryMatchesContext(this.summary, this.approval, this.context)
          ? this.summary
          : null;
      if (hostSummary?.phase === 'bootstrapping') {
        statusText = 'Host starting';
        statusTone = 'active';
      } else if (hostSummary?.phase === 'failed') {
        statusText = 'Host failed';
        statusTone = 'warn';
      } else if (hostSummary?.phase === 'stopped') {
        statusText = 'Host stopped';
      } else if (hostSummary?.phase === 'ready') {
        statusText = 'Host not ready';
        statusTone = 'warn';
      } else {
        statusText = 'Waiting for Ready';
      }
    }

    setText(view.badge, statusText);
    view.badge.setAttribute('data-status', statusTone);

    const brand = resolveHostTaskBrand(snapshot);
    if (view.brand !== brand) {
      view.brandHost.replaceChildren(createBrandIconElement(brand));
      view.brand = brand;
    }

    if (snapshot) {
      const telemetry = formatTaskTelemetry(snapshot);
      setText(
        view.meta,
        `${formatAdapterLabel(
          snapshot.adapter
        )} · requested ${telemetry.requested} · observed ${telemetry.observed}`
      );
      setText(view.requested, telemetry.requested);
      setText(view.observed, telemetry.observed);
      setText(
        view.progress,
        compactDisplayText(formatTaskProgress(snapshot), PROGRESS_DISPLAY_LIMIT)
      );
      setText(
        view.instruction,
        compactDisplayText(
          formatTaskInstruction(snapshot),
          INSTRUCTION_DISPLAY_LIMIT
        )
      );
      const outcomeText = formatTaskOutcome(snapshot);
      setText(
        view.outcome,
        outcomeText || 'No terminal outcome reported.'
      );
      const reviewText = taskReviewNote(snapshot);
      setText(view.review, reviewText);
      view.review.hidden = reviewText.length === 0;
    } else {
      setText(
        view.meta,
        error
          ? error
          : pending
            ? 'Reading approved task status…'
            : !hasTauriRuntime()
              ? 'Actual task status requires the native Tauri desktop bridge.'
              : !this.currentReadySession()
                ? 'Actual provider telemetry appears only after the host reports Ready.'
                : 'No task snapshot has been reported yet.'
      );
      setText(view.requested, 'Not reported');
      setText(view.observed, 'Not reported');
      setText(
        view.progress,
        error ?? 'Not available before the host reports Ready.'
      );
      setText(view.instruction, 'Not available before dispatch.');
      setText(view.outcome, 'Not available before dispatch.');
      setText(view.review, '');
      view.review.hidden = true;
    }

    view.root.setAttribute('data-task-id', taskId);
  }

  private syncTaskRows(): void {
    const approval = this.approval;
    if (!approval) {
      this.tasksSection.hidden = true;
      return;
    }
    this.tasksSection.hidden = false;

    const taskIds = normalizeApprovedTaskIds(approval.approved_task_ids);
    const visibleIds = taskIds.slice(0, this.visibleTaskCount);
    const visibleSet = new Set(visibleIds);

    for (const [taskId, view] of this.taskRowViews.entries()) {
      if (!visibleSet.has(taskId)) {
        view.root.remove();
        this.taskRowViews.delete(taskId);
      }
    }

    for (const taskId of visibleIds) {
      if (!this.taskRowViews.has(taskId)) {
        this.taskRowViews.set(taskId, this.createTaskRowView(taskId));
      }
    }

    const orderedRows = visibleIds
      .map((taskId) => this.taskRowViews.get(taskId)?.root)
      .filter((row): row is HTMLElement => row !== undefined);
    const currentRows = Array.from(this.tasksList.children);
    const orderChanged =
      currentRows.length !== orderedRows.length ||
      orderedRows.some((row, index) => currentRows[index] !== row);
    if (orderChanged) {
      this.tasksList.replaceChildren(...orderedRows);
    }

    for (const taskId of visibleIds) {
      const view = this.taskRowViews.get(taskId);
      if (!view) {
        continue;
      }
      this.syncTaskRow(
        view,
        taskId,
        this.taskSnapshots.get(taskId) ?? null,
        this.taskPending.has(taskId),
        this.taskErrors.get(taskId) ?? null
      );
    }

    setText(this.tasksCount, `${taskIds.length} approved`);
    const remaining = taskIds.length - visibleIds.length;
    this.moreButton.hidden = remaining <= 0;
    setText(
      this.moreButton,
      `Show ${Math.min(
        MAX_VISIBLE_APPROVED_TASKS,
        Math.max(0, remaining)
      )} more`
    );

    if (taskIds.length === 0) {
      setText(this.tasksHint, 'No task IDs were frozen in this approval.');
    } else if (!hasTauriRuntime()) {
      setText(
        this.tasksHint,
        'Read-only: task status requires the native Tauri desktop bridge.'
      );
    } else if (
      !this.context ||
      !approvalContextMatches(approval, this.context)
    ) {
      setText(
        this.tasksHint,
        'Task status requires the exact approved control binding and native thread.'
      );
    } else if (!this.currentReadySession()) {
      setText(
        this.tasksHint,
        'Task status reads begin only when the backend reports authenticated Ready.'
      );
    } else {
      setText(
        this.tasksHint,
        'Read-only task status. Dispatch stays with the approved MCP workflow; the backend revalidates this approval.'
      );
    }
  }

  private syncTechSection(): void {
    const approval = this.approval;
    const context = this.context;
    const summary = this.summary;
    if (
      !approval ||
      !context ||
      !summary ||
      !hostSummaryMatchesContext(summary, approval, context)
    ) {
      this.techSection.hidden = true;
      this.techBody.replaceChildren();
      return;
    }
    this.techSection.hidden = false;

    const gates = summary.gates;
    const rows: HTMLElement[] = [
      kvRow('Control binding', summary.binding_id),
      kvRow('Host session binding', summary.session_binding_id ?? 'Not created'),
      kvRow('Plan revision', `${summary.plan_id} · r${summary.plan_revision}`),
      kvRow(
        'Routes',
        `${summary.worker_profile_count} worker · ${summary.native_route_count} native`
      ),
      kvRow(
        'Task partitions',
        `${summary.root_worker_task_ids.length} root · ${
          summary.dependent_task_ids.length
        } dependent · ${summary.native_route_task_ids.length} native route`
      ),
      kvRow(
        'Authenticated supervisor ready',
        gates.supervisor_authenticated_ready ? 'Yes' : 'No'
      ),
      kvRow(
        'Workers auto-started on boot',
        gates.workers_auto_started_on_boot ? 'Yes' : 'No'
      ),
      kvRow('Root tasks only', gates.root_tasks_only ? 'Yes' : 'No'),
      kvRow(
        'Dependent tasks blocked',
        gates.dependent_tasks_blocked ? 'Yes' : 'No'
      ),
      kvRow(
        'Native routes manual only',
        gates.native_routes_manual_only ? 'Yes' : 'No'
      ),
      kvRow(
        'Worker completion means review',
        gates.worker_completed_means_ready_for_review ? 'Yes' : 'No'
      ),
      kvRow(
        'Auto accept or merge enabled',
        gates.auto_accept_or_merge_enabled ? 'Yes' : 'No'
      ),
    ];
    if (summary.last_error?.trim()) {
      rows.push(
        kvRow('Reported diagnostic', compactDisplayText(summary.last_error, 240))
      );
    }
    this.techBody.replaceChildren(
      h('dl', { className: 'aq-host-session-kv' }, ...rows)
    );
  }

  private syncUi(): void {
    if (this.disposed) {
      return;
    }
    const approval = this.approval;
    const context = this.context;
    const nativeBridgeAvailable = hasTauriRuntime();
    const contextMatches =
      approval !== null && context !== null && approvalContextMatches(approval, context);
    const activeSummary =
      nativeBridgeAvailable &&
      this.summary &&
      approval !== null &&
      context !== null &&
      hostSummaryMatchesContext(this.summary, approval, context)
        ? this.summary
        : null;
    const ready = this.currentReadySession();
    const startMatchesContext =
      this.startInFlight && this.startRequestKey === this.currentRequestKey();

    let summaryBadgeText = 'Not checked';
    let summaryBadgeTone: 'default' | 'active' | 'warn' = 'default';
    if (startMatchesContext) {
      summaryBadgeText = 'Starting…';
      summaryBadgeTone = 'active';
    } else if (this.statusLoading && this.statusReason !== 'poll') {
      summaryBadgeText = 'Reading…';
      summaryBadgeTone = 'active';
    } else if (activeSummary) {
      summaryBadgeText = formatHostStatusLabel(activeSummary);
      summaryBadgeTone = formatHostStatusTone(activeSummary);
    } else if (this.errorMessage) {
      summaryBadgeText =
        this.lastErrorKind === 'not_ready'
          ? 'Host not ready'
          : this.lastErrorKind === 'bridge_disconnected'
            ? 'Bridge disconnected'
            : 'Status unavailable';
      summaryBadgeTone = 'warn';
    } else if (!nativeBridgeAvailable || !contextMatches) {
      summaryBadgeText = 'Not checked';
    } else if (this.root.open) {
      summaryBadgeText = 'Not started';
    }
    setText(this.summaryBadge, summaryBadgeText);
    this.summaryBadge.setAttribute('data-status', summaryBadgeTone);

    let notice = '';
    if (!nativeBridgeAvailable) {
      notice = isDevPreviewFixtureMode()
        ? 'Read-only preview fixture. Local sessions and task status require the native Tauri desktop bridge.'
        : 'Read-only browser view. Start and task status require the native Tauri desktop bridge.';
    } else if (!approval || !context) {
      notice = 'Select an approved conversation binding to inspect its local session.';
    } else if (!context.connected) {
      notice =
        'Runtime offline. Connect the exact approved control binding before starting a local session.';
    } else if (!contextMatches) {
      notice =
        'Current conversation does not match this approval control binding and native thread.';
    }
    setText(this.notice, notice);
    this.notice.hidden = notice.length === 0;

    let statusText =
      nativeBridgeAvailable && contextMatches ? 'Not started' : 'Not checked';
    let statusTone: 'default' | 'active' | 'warn' = 'default';
    if (startMatchesContext) {
      statusText = 'Starting…';
      statusTone = 'active';
    } else if (this.statusLoading && this.statusReason !== 'poll') {
      statusText = 'Reading…';
      statusTone = 'active';
    } else if (ready) {
      statusText = 'Ready';
      statusTone = 'active';
    } else if (activeSummary) {
      statusText = formatHostStatusLabel(activeSummary);
      statusTone = formatHostStatusTone(activeSummary);
    } else if (this.errorMessage) {
      if (this.lastErrorKind === 'not_ready') {
        statusText = 'Host not ready';
      } else if (this.lastErrorKind === 'bridge_disconnected') {
        statusText = 'Bridge disconnected';
      } else {
        statusText = 'Status unavailable';
      }
      statusTone = 'warn';
    }
    setText(this.statusBadge, statusText);
    this.statusBadge.setAttribute('data-status', statusTone);

    let meta = '';
    if (startMatchesContext) {
      meta = 'Starting the approved host process. No models or tasks are started.';
    } else if (this.statusLoading && this.statusReason !== 'poll') {
      meta = 'Reading approved host status…';
    } else if (this.errorMessage) {
      meta = this.errorMessage;
    } else if (!nativeBridgeAvailable) {
      meta = 'Native Tauri bridge required.';
    } else if (!contextMatches) {
      meta = 'The exact approved control binding and native thread are required.';
    } else if (activeSummary) {
      const workerLabel =
        activeSummary.worker_profile_count === 1 ? 'worker route' : 'worker routes';
      const nativeLabel =
        activeSummary.native_route_count === 1 ? 'native route' : 'native routes';
      meta = `${activeSummary.plan_id} r${activeSummary.plan_revision} · ${
        activeSummary.worker_profile_count
      } ${workerLabel} · ${activeSummary.native_route_count} ${nativeLabel}`;
      if (activeSummary.last_error?.trim() && !isHostSummaryReady(activeSummary)) {
        meta += ' · Reported diagnostic in Session details';
      }
    } else {
      meta = 'No local session has been started for this approval.';
    }
    setText(this.statusMeta, meta);

    this.refreshButton.hidden = !(
      this.root.open &&
      nativeBridgeAvailable &&
      contextMatches
    );
    this.refreshButton.disabled = this.statusLoading || this.startInFlight;

    const canStart =
      approval !== null &&
      context !== null &&
      canStartLocalSession(approval, context, {
        nativeBridgeAvailable,
        busy:
          this.statusLoading ||
          this.startInFlight ||
          this.lastErrorKind === 'bridge_disconnected',
        summary: activeSummary,
      });
    const showStart =
      this.root.open && canStart && nativeBridgeAvailable && contextMatches;
    this.startButton.hidden = !showStart;
    this.startButton.disabled = this.statusLoading || this.startInFlight;
    setText(this.startButtonLabel, 'Start local session');
    this.actions.hidden = this.refreshButton.hidden && this.startButton.hidden;

    this.syncTechSection();
    this.syncTaskRows();
  }
}
