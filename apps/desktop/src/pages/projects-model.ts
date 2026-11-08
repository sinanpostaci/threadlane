/**
 * Pure projection helpers for the read-only Projects -> Sessions -> Calls view.
 *
 * This module has no DOM, Tauri, shell, or provider imports so session grouping,
 * deduplication, retention windows, and provenance formatting can be validated
 * directly under node:test.
 */
import type { BrandKey } from '../types.ts';
import type {
  ObservationCall,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
  ObservationUsage,
} from '../observation/types.ts';
import {
  compactFeedText,
  formatCompactDuration,
  formatCompactUsage,
  formatDurationDetail,
  formatExecutorRoleLabel,
  formatExecutorSummary,
  formatFriendlyModel,
  formatLifecycleLabel,
  formatRecordedOperation,
  formatRequestedObserved,
  hasExecutorRecord,
  isLocalMcpExecutor,
  projectDuration,
  projectUsage,
  resolveExecutorBrand,
  resolveLifecycleTone,
  shouldShowSpinner,
  type ActivityFeedScope,
  type LifecycleTone,
} from './activity-feed-model.ts';
import {
  formatJobLifecycle,
  jobPlanKey,
  summarizeJobProgress,
} from './job-plans-model.ts';

export type ProjectsWindowHours = 6 | 24;

export interface ProjectCallView {
  key: string;
  title: string;
  role: string;
  model: string;
  effort: string | null;
  brand: BrandKey;
  stateLabel: string;
  tone: LifecycleTone;
  running: boolean;
  durationLabel: string;
  usageLabel: string;
  detail: string;
  startedAtMs: number | null;
  finishedAtMs: number | null;
}

export interface ProjectEventView {
  key: string;
  label: string;
  atMs: number | null;
  detail: string;
}

export interface ProjectJobView {
  key: string;
  job: ObservationJob;
  stateLabel: string;
  taskSummary: string;
  attention: boolean;
  planLabel: string | null;
}

export interface ProjectSessionView {
  key: string;
  label: string;
  detail: string;
  threadUuid: string | null;
  bindingId: string | null;
  updatedAtMs: number | null;
  stateLabel: string;
  tone: LifecycleTone;
  runningCount: number;
  attention: boolean;
  jobs: ProjectJobView[];
  calls: ProjectCallView[];
  recent: ProjectEventView[];
}

export interface ProjectGroupView {
  key: string;
  label: string;
  path: string | null;
  sessions: ProjectSessionView[];
  runningCount: number;
}

export interface ProjectsProjection {
  projects: ProjectGroupView[];
  runningCount: number;
  hiddenSessionCount: number;
  unavailableSourceCount: number;
  warnings: string[];
  hasMore: boolean;
  jobsHasMore: boolean;
  emptyMessage: string | null;
}

export interface ProjectProjectsOptions {
  nowMs?: number;
  windowHours?: ProjectsWindowHours;
  scope?: ActivityFeedScope | null;
}

const MAX_RECENT_EVENTS_PER_SESSION = 6;
const MAX_TERMINAL_CALLS_PER_SESSION = 12;

function trimOrNull(value: string | null | undefined): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function usableChatLabel(value: string | null | undefined): string | null {
  const label = trimOrNull(value);
  if (label === null) {
    return null;
  }
  return label.replace(/\s+/g, ' ').toLowerCase() === 'codex chat'
    ? null
    : label;
}

function capitalizeLabel(value: string): string {
  return value.length > 0 ? value.charAt(0).toUpperCase() + value.slice(1) : value;
}

function compactStableIdentifier(value: string): string {
  const trimmed = value.trim();
  return trimmed.length <= 12 ? trimmed : trimmed.slice(0, 8);
}

function compareText(left: string, right: string): number {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

function compareCaseInsensitiveThenExact(left: string, right: string): number {
  return (
    compareText(left.toLowerCase(), right.toLowerCase()) ||
    compareText(left, right)
  );
}

/**
 * Accepts only finite nonnegative safe millisecond timestamps.
 */
export function safeTimestampMs(value: number | null | undefined): number | null {
  if (
    typeof value !== 'number' ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > Number.MAX_SAFE_INTEGER
  ) {
    return null;
  }
  return value;
}

function maxTimestampMs(
  values: ReadonlyArray<number | null | undefined>
): number | null {
  let newest: number | null = null;
  for (const candidate of values) {
    const ms = safeTimestampMs(candidate);
    if (ms !== null && (newest === null || ms > newest)) {
      newest = ms;
    }
  }
  return newest;
}

function requestSafeMeaningfulTimestampMs(
  request: ObservationRequest
): number | null {
  const candidates: Array<number | null | undefined> = [
    request.updated_at_ms,
    request.finished_at_ms,
    request.started_at_ms,
  ];
  for (const delivery of request.delivery ?? []) {
    candidates.push(delivery.at_ms);
  }
  for (const child of request.children ?? []) {
    candidates.push(child.finished_at_ms, child.started_at_ms);
  }
  return maxTimestampMs(candidates);
}

export function projectJobLookupKey(sourceId: string, jobId: string): string {
  return JSON.stringify([sourceId, jobId]);
}

export function projectRequestLookupKey(
  sourceId: string,
  requestId: string
): string {
  return JSON.stringify(['request', sourceId, requestId]);
}

export function projectGroupIdentityKey(
  sourceId: string,
  projectPath: string | null,
  projectLabel: string
): string {
  if (projectPath !== null) {
    return JSON.stringify(['project-path', projectPath]);
  }
  return JSON.stringify(['project-label', sourceId, projectLabel]);
}

function deduplicateJobs(jobs: readonly ObservationJob[]): ObservationJob[] {
  const byKey = new Map<string, ObservationJob>();
  for (const job of jobs) {
    const key = projectJobLookupKey(job.source_id, job.id);
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, job);
      continue;
    }
    const existingMs = safeTimestampMs(existing.updated_at_ms) ?? -1;
    const nextMs = safeTimestampMs(job.updated_at_ms) ?? -1;
    if (nextMs > existingMs) {
      byKey.set(key, job);
    }
  }
  return Array.from(byKey.values());
}

function deduplicateRequests(
  requests: readonly ObservationRequest[]
): ObservationRequest[] {
  const byKey = new Map<string, ObservationRequest>();
  for (const request of requests) {
    const key = projectRequestLookupKey(request.source_id, request.id);
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, request);
      continue;
    }
    const existingMs = requestSafeMeaningfulTimestampMs(existing) ?? -1;
    const nextMs = requestSafeMeaningfulTimestampMs(request) ?? -1;
    if (nextMs > existingMs) {
      byKey.set(key, request);
    }
  }
  return Array.from(byKey.values());
}

function isProviderExecutor(
  executor: ObservationExecutor | null | undefined
): boolean {
  return hasExecutorRecord(executor) && !isLocalMcpExecutor(executor);
}

function formatCallModel(
  executor: ObservationExecutor | null | undefined
): string {
  const observed = trimOrNull(executor?.observed_model);
  const requested = trimOrNull(executor?.requested_model);
  if (observed !== null && requested !== null && observed !== requested) {
    return `req ${formatFriendlyModel(requested)} · obs ${formatFriendlyModel(observed)}`;
  }
  const chosen = observed ?? requested;
  return chosen !== null ? formatFriendlyModel(chosen) : 'Model not reported';
}

function resolveCallEffort(
  executor: ObservationExecutor | null | undefined
): string | null {
  const observedModel = trimOrNull(executor?.observed_model);
  if (observedModel !== null) {
    return (
      trimOrNull(executor?.observed_effort) ??
      trimOrNull(executor?.requested_effort)
    );
  }
  return (
    trimOrNull(executor?.observed_effort) ??
    trimOrNull(executor?.requested_effort)
  );
}

function formatCallUsage(
  usage: ObservationUsage | null | undefined
): { label: string; detail: string } {
  const usageView = projectUsage(usage);
  if (!usageView.reported) {
    return {
      label: 'Usage unknown',
      detail: 'Usage unknown · Usage not reported.',
    };
  }
  if (usageView.total !== null) {
    return {
      label: formatCompactUsage(usageView).value,
      detail: usageView.detail,
    };
  }
  return {
    label: usageView.summary,
    detail: usageView.detail,
  };
}

interface CallCandidate {
  callKeyId: string;
  sourceId: string;
  state: ObservationRequest['state'];
  view: ProjectCallView;
  sortAtMs: number | null;
  fromChild: boolean;
}

function buildCallCandidateFromChild(
  sourceId: string,
  parentRequest: ObservationRequest,
  call: ObservationCall,
  nowMs: number
): CallCandidate {
  const executor = call.executor ?? null;
  const duration = projectDuration(call, nowMs);
  const durationDetail = formatDurationDetail(duration);
  const durationCompact = formatCompactDuration(duration, durationDetail);
  const usage = formatCallUsage(call.usage);
  const startedAtMs = safeTimestampMs(call.started_at_ms);
  const finishedAtMs = safeTimestampMs(call.finished_at_ms);
  const summary = compactFeedText(call.summary, 320);
  const role = capitalizeLabel(formatExecutorRoleLabel(executor?.role));
  const model = formatCallModel(executor);
  const effort = resolveCallEffort(executor);
  const detailParts = [
    `${formatExecutorSummary(executor)}.`,
    `Model: ${formatRequestedObserved(executor?.requested_model, executor?.observed_model)}.`,
    `Effort: ${formatRequestedObserved(executor?.requested_effort, executor?.observed_effort)}.`,
    durationDetail,
    usage.detail,
    summary ? `Summary: ${summary}` : '',
    `Source: ${sourceId} · Call: ${call.id}`,
  ].filter((part) => part.length > 0);

  return {
    callKeyId: call.id,
    sourceId,
    state: call.state,
    sortAtMs:
      maxTimestampMs([finishedAtMs, startedAtMs]) ??
      safeTimestampMs(parentRequest.updated_at_ms),
    fromChild: true,
    view: {
      key: JSON.stringify(['call', sourceId, call.id]),
      title:
        trimOrNull(call.title) ??
        trimOrNull(parentRequest.title) ??
        'Call title not reported',
      role,
      model,
      effort,
      brand: resolveExecutorBrand(executor),
      stateLabel: formatLifecycleLabel(call.state),
      tone: resolveLifecycleTone(call.state),
      running: shouldShowSpinner(call.state),
      durationLabel: durationCompact.value,
      usageLabel: usage.label,
      detail: detailParts.join(' '),
      startedAtMs,
      finishedAtMs,
    },
  };
}

function buildCallCandidateFromRequest(
  request: ObservationRequest,
  nowMs: number
): CallCandidate {
  const executor = request.executor ?? null;
  const duration = projectDuration(request, nowMs);
  const durationDetail = formatDurationDetail(duration);
  const durationCompact = formatCompactDuration(duration, durationDetail);
  const usage = formatCallUsage(request.usage);
  const startedAtMs = safeTimestampMs(request.started_at_ms);
  const finishedAtMs = safeTimestampMs(request.finished_at_ms);
  const summary = compactFeedText(request.summary, 320);
  const role = capitalizeLabel(formatExecutorRoleLabel(executor?.role));
  const model = formatCallModel(executor);
  const effort = resolveCallEffort(executor);
  const detailParts = [
    `${formatExecutorSummary(executor)}.`,
    `Model: ${formatRequestedObserved(executor?.requested_model, executor?.observed_model)}.`,
    `Effort: ${formatRequestedObserved(executor?.requested_effort, executor?.observed_effort)}.`,
    durationDetail,
    usage.detail,
    summary ? `Summary: ${summary}` : '',
    `Source: ${request.source_id} · Request: ${request.id}`,
  ].filter((part) => part.length > 0);

  return {
    callKeyId: request.id,
    sourceId: request.source_id,
    state: request.state,
    sortAtMs: requestSafeMeaningfulTimestampMs(request),
    fromChild: false,
    view: {
      key: JSON.stringify(['call', request.source_id, request.id]),
      title:
        trimOrNull(request.title) ??
        capitalizeLabel(formatRecordedOperation(request.operation)),
      role,
      model,
      effort,
      brand: resolveExecutorBrand(executor),
      stateLabel: formatLifecycleLabel(request.state),
      tone: resolveLifecycleTone(request.state),
      running: shouldShowSpinner(request.state),
      durationLabel: durationCompact.value,
      usageLabel: usage.label,
      detail: detailParts.join(' '),
      startedAtMs,
      finishedAtMs,
    },
  };
}

function shouldReplaceCallCandidate(
  existing: CallCandidate,
  incoming: CallCandidate
): boolean {
  if (existing.view.running !== incoming.view.running) {
    return incoming.view.running;
  }
  const existingMs = existing.sortAtMs ?? -1;
  const incomingMs = incoming.sortAtMs ?? -1;
  if (incomingMs !== existingMs) {
    return incomingMs > existingMs;
  }
  if (existing.fromChild !== incoming.fromChild) {
    return incoming.fromChild;
  }
  return false;
}

function isTerminalLifecycleState(
  state: ObservationRequest['state']
): boolean {
  return state === 'completed' || state === 'failed' || state === 'cancelled';
}

function callSortPriority(candidate: CallCandidate): number {
  if (candidate.view.running) {
    return 0;
  }
  if (!isTerminalLifecycleState(candidate.state)) {
    return 1;
  }
  return 2;
}

function buildEventFromRequest(request: ObservationRequest): ProjectEventView {
  const operation = capitalizeLabel(formatRecordedOperation(request.operation));
  const title = trimOrNull(request.title) ?? operation;
  const stateLabel = formatLifecycleLabel(request.state);
  const summary = compactFeedText(request.summary, 240);
  const detail = [
    `Operation: ${operation}`,
    `State: ${stateLabel}`,
    summary,
    `Source: ${request.source_id} · Request: ${request.id}`,
  ]
    .filter((item) => item.length > 0)
    .join(' · ');

  return {
    key: JSON.stringify(['event', request.source_id, request.id]),
    label: title,
    atMs: requestSafeMeaningfulTimestampMs(request),
    detail,
  };
}

function buildEventFromLocalChild(
  request: ObservationRequest,
  call: ObservationCall
): ProjectEventView {
  const title = trimOrNull(call.title) ?? 'Local operation';
  const stateLabel = formatLifecycleLabel(call.state);
  const summary = compactFeedText(call.summary, 240);
  const detail = [
    'Threadlane MCP · local',
    `State: ${stateLabel}`,
    summary,
    `Source: ${request.source_id} · Call: ${call.id}`,
  ]
    .filter((item) => item.length > 0)
    .join(' · ');

  return {
    key: JSON.stringify([
      'event-child',
      request.source_id,
      request.id,
      call.id,
    ]),
    label: title,
    atMs:
      maxTimestampMs([call.finished_at_ms, call.started_at_ms]) ??
      safeTimestampMs(request.updated_at_ms),
    detail,
  };
}

function isUnresolvedApprovalStatus(status: string | null | undefined): boolean {
  const normalized = trimOrNull(status)?.toLowerCase();
  if (!normalized) {
    return false;
  }
  return (
    normalized === 'pending' ||
    normalized === 'needs_user' ||
    normalized === 'awaiting_approval' ||
    normalized === 'approval_required' ||
    normalized === 'requested' ||
    normalized === 'required' ||
    normalized === 'unapproved' ||
    normalized === 'ready'
  );
}

function isUnresolvedReviewState(state: string | null | undefined): boolean {
  const normalized = trimOrNull(state)?.toLowerCase();
  if (!normalized) {
    return false;
  }
  return (
    normalized === 'ready_for_review' ||
    normalized === 'needs_review' ||
    normalized === 'reviewing' ||
    normalized === 'pending' ||
    normalized === 'changes_requested'
  );
}

function isJobActive(job: ObservationJob): boolean {
  return (
    job.state === 'planning' ||
    job.state === 'running' ||
    job.state === 'reviewing' ||
    (Number.isSafeInteger(job.active_call_count) && job.active_call_count > 0)
  );
}

function isJobAttention(job: ObservationJob): boolean {
  if (
    job.state === 'needs_user' ||
    job.state === 'ready' ||
    job.state === 'reviewing' ||
    job.state === 'unknown'
  ) {
    return true;
  }
  const isTerminal = job.state === 'completed' || job.state === 'failed';
  if (!isTerminal) {
    if (
      isUnresolvedApprovalStatus(job.approval_status) ||
      isUnresolvedReviewState(job.review_state)
    ) {
      return true;
    }
  }
  return false;
}

function formatJobPlanLabel(job: ObservationJob): string | null {
  const planId = trimOrNull(job.plan_id);
  const revision =
    typeof job.plan_revision === 'number' && Number.isFinite(job.plan_revision)
      ? Math.trunc(job.plan_revision)
      : null;
  if (revision !== null) {
    return `Plan · revision ${revision}`;
  }
  if (planId !== null) {
    return 'Plan available';
  }
  return null;
}

function buildProjectJobView(job: ObservationJob): ProjectJobView {
  const taskSummary = summarizeJobProgress(job) || 'No tasks recorded';
  return {
    key: jobPlanKey(job),
    job,
    stateLabel: formatJobLifecycle(job.state),
    taskSummary,
    attention: isJobAttention(job),
    planLabel: formatJobPlanLabel(job),
  };
}

interface ResolvedProjectIdentity {
  key: string;
  label: string;
  path: string | null;
}

interface SessionAccumulator {
  key: string;
  sourceId: string;
  project: ResolvedProjectIdentity;
  threadUuid: string | null;
  bindingId: string | null;
  isolatedKind: 'job' | 'request' | null;
  isolatedId: string | null;
  threadLabels: string[];
  jobs: ObservationJob[];
  requests: ObservationRequest[];
}

function resolveSessionSummaryState(
  jobs: readonly ProjectJobView[],
  calls: readonly ProjectCallView[],
  requests: readonly ObservationRequest[],
  runningCount: number
): { stateLabel: string; tone: LifecycleTone; attention: boolean } {
  const hasNeedsUserJob = jobs.some((item) => item.job.state === 'needs_user');
  const hasWaitingCallOrRequest =
    calls.some((call) => call.stateLabel === 'Waiting for user') ||
    requests.some((req) => req.state === 'waiting_for_user');
  const hasReviewingJob = jobs.some(
    (item) =>
      item.job.state === 'reviewing' ||
      (item.job.state !== 'completed' &&
        item.job.state !== 'failed' &&
        isUnresolvedReviewState(item.job.review_state))
  );
  const hasReadyForReviewCallOrRequest =
    calls.some((call) => call.stateLabel === 'Ready for review') ||
    requests.some((req) => req.state === 'ready_for_review');
  const hasReadyJob = jobs.some(
    (item) =>
      item.job.state === 'ready' ||
      (item.job.state !== 'completed' &&
        item.job.state !== 'failed' &&
        trimOrNull(item.job.plan_id) !== null &&
        isUnresolvedApprovalStatus(item.job.approval_status))
  );
  const hasPlanningJob = jobs.some((item) => item.job.state === 'planning');
  const hasRunningJob = jobs.some((item) => item.job.state === 'running');
  const hasQueuedCall =
    calls.some((call) => call.stateLabel === 'Queued') ||
    requests.some(
      (req) => !isLocalMcpExecutor(req.executor) && req.state === 'queued'
    );
  const hasUnknown =
    jobs.some((item) => item.job.state === 'unknown') ||
    calls.some((call) => call.stateLabel === 'Unknown') ||
    requests.some((req) => req.state === 'unknown');
  const hasFailed =
    jobs.some((item) => item.job.state === 'failed') ||
    calls.some((call) => call.stateLabel === 'Failed') ||
    requests.some((req) => req.state === 'failed');
  const hasCancelled =
    calls.some((call) => call.stateLabel === 'Cancelled') ||
    requests.some((req) => req.state === 'cancelled');

  const attention =
    jobs.some((item) => item.attention) ||
    hasNeedsUserJob ||
    hasWaitingCallOrRequest ||
    hasReviewingJob ||
    hasReadyForReviewCallOrRequest ||
    hasReadyJob ||
    hasUnknown ||
    hasFailed;

  if (runningCount > 0 || hasRunningJob) {
    return {
      stateLabel: hasFailed ? 'Running · Needs attention' : 'Running',
      tone: hasFailed ? 'danger' : 'active',
      attention,
    };
  }
  if (hasNeedsUserJob) {
    return { stateLabel: 'Needs you', tone: 'waiting', attention: true };
  }
  if (hasWaitingCallOrRequest) {
    return { stateLabel: 'Waiting for user', tone: 'waiting', attention: true };
  }
  if (hasReviewingJob) {
    return { stateLabel: 'Reviewing', tone: 'review', attention: true };
  }
  if (hasReadyForReviewCallOrRequest) {
    return { stateLabel: 'Ready for review', tone: 'review', attention: true };
  }
  if (hasReadyJob) {
    return { stateLabel: 'Ready', tone: 'waiting', attention: true };
  }
  if (hasPlanningJob) {
    return { stateLabel: 'Planning', tone: 'active', attention };
  }
  if (hasQueuedCall) {
    return { stateLabel: 'Queued', tone: 'neutral', attention };
  }
  if (hasUnknown) {
    return { stateLabel: 'Unknown', tone: 'unknown', attention: true };
  }
  if (hasFailed) {
    return { stateLabel: 'Failed', tone: 'danger', attention };
  }
  if (jobs.length > 0) {
    return { stateLabel: 'Completed', tone: 'ok', attention };
  }
  if (
    hasCancelled &&
    !calls.some((call) => call.stateLabel === 'Completed') &&
    !requests.some((req) => req.state === 'completed')
  ) {
    return { stateLabel: 'Cancelled', tone: 'cancelled', attention };
  }
  return { stateLabel: 'Completed', tone: 'ok', attention };
}

function shouldRetainSessionRegardlessOfWindow(
  accumulator: SessionAccumulator,
  jobViews: readonly ProjectJobView[],
  callCandidates: readonly CallCandidate[]
): boolean {
  if (accumulator.jobs.some((job) => isJobActive(job) || isJobAttention(job))) {
    return true;
  }
  if (jobViews.some((job) => job.attention)) {
    return true;
  }
  for (const candidate of callCandidates) {
    if (!isTerminalLifecycleState(candidate.state)) {
      return true;
    }
  }
  for (const request of accumulator.requests) {
    if (
      request.state === 'waiting_for_user' ||
      request.state === 'ready_for_review' ||
      request.state === 'unknown'
    ) {
      return true;
    }
    if (
      !isLocalMcpExecutor(request.executor) &&
      hasExecutorRecord(request.executor) &&
      (request.state === 'running' || request.state === 'queued')
    ) {
      return true;
    }
  }
  return false;
}

export function projectProjects(
  snapshot: ObservationSnapshot,
  options: ProjectProjectsOptions = {}
): ProjectsProjection {
  const scope = options.scope ?? null;
  const windowHours: ProjectsWindowHours = options.windowHours === 24 ? 24 : 6;
  const nowMs =
    safeTimestampMs(options.nowMs) ??
    safeTimestampMs(snapshot.generated_at_ms) ??
    Date.now();
  const cutoffMs = nowMs - windowHours * 60 * 60 * 1000;

  const sources = snapshot.sources ?? [];
  const sourcesById = new Map<string, ObservationSource>(
    sources.map((source) => [source.id, source] as const)
  );

  const uniqueJobs = deduplicateJobs(snapshot.jobs ?? []);
  const uniqueRequests = deduplicateRequests(snapshot.requests ?? []);
  const allJobsByKey = new Map<string, ObservationJob>(
    uniqueJobs.map(
      (job) => [projectJobLookupKey(job.source_id, job.id), job] as const
    )
  );

  const scopedJobs = scope
    ? uniqueJobs.filter(
        (job) => job.source_id === scope.source_id && job.id === scope.job_id
      )
    : uniqueJobs;
  const scopedRequests = scope
    ? uniqueRequests.filter(
        (request) =>
          request.source_id === scope.source_id && request.job_id === scope.job_id
      )
    : uniqueRequests;

  const distinctLabelsBySource = new Map<string, Set<string>>();
  const addSourceLabel = (sourceId: string, label: string | null) => {
    if (!label) {
      return;
    }
    let set = distinctLabelsBySource.get(sourceId);
    if (!set) {
      set = new Set<string>();
      distinctLabelsBySource.set(sourceId, set);
    }
    set.add(label);
  };
  for (const job of uniqueJobs) {
    addSourceLabel(job.source_id, trimOrNull(job.project_label));
  }
  for (const request of uniqueRequests) {
    addSourceLabel(request.source_id, trimOrNull(request.project_label));
  }

  const canUseSourceDefaultPath = (
    sourceId: string,
    projectLabel: string
  ): boolean => {
    const source = sourcesById.get(sourceId);
    const sourcePath = trimOrNull(source?.project_path);
    if (sourcePath === null) {
      return false;
    }
    const sourceLabel = trimOrNull(source?.label);
    if (sourceLabel !== null && sourceLabel === projectLabel) {
      return true;
    }
    const labels = distinctLabelsBySource.get(sourceId);
    return !labels || labels.size <= 1;
  };

  const resolveJobProject = (job: ObservationJob): ResolvedProjectIdentity => {
    const source = sourcesById.get(job.source_id);
    const label =
      trimOrNull(job.project_label) ??
      trimOrNull(source?.label) ??
      trimOrNull(job.project_path) ??
      'Project not reported';
    const path =
      trimOrNull(job.project_path) ??
      (canUseSourceDefaultPath(job.source_id, label)
        ? trimOrNull(source?.project_path)
        : null);
    return {
      key: projectGroupIdentityKey(job.source_id, path, label),
      label,
      path,
    };
  };

  const jobPathsBySourceLabel = new Map<string, Set<string>>();
  for (const job of uniqueJobs) {
    const resolved = resolveJobProject(job);
    if (resolved.path !== null) {
      const key = JSON.stringify([job.source_id, resolved.label]);
      let paths = jobPathsBySourceLabel.get(key);
      if (!paths) {
        paths = new Set<string>();
        jobPathsBySourceLabel.set(key, paths);
      }
      paths.add(resolved.path);
    }
  }

  const resolveRequestProject = (
    request: ObservationRequest
  ): ResolvedProjectIdentity => {
    const source = sourcesById.get(request.source_id);
    const linkedJobId = trimOrNull(request.job_id);
    const linkedJob = linkedJobId
      ? allJobsByKey.get(projectJobLookupKey(request.source_id, linkedJobId)) ??
        null
      : null;
    if (linkedJob) {
      return resolveJobProject(linkedJob);
    }
    const label =
      trimOrNull(request.project_label) ??
      trimOrNull(source?.label) ??
      'Project not reported';
    const matchingJobPaths = jobPathsBySourceLabel.get(
      JSON.stringify([request.source_id, label])
    );
    const unambiguousJobPath =
      matchingJobPaths && matchingJobPaths.size === 1
        ? Array.from(matchingJobPaths)[0] ?? null
        : null;
    const path =
      unambiguousJobPath ??
      (canUseSourceDefaultPath(request.source_id, label)
        ? trimOrNull(source?.project_path)
        : null);
    return {
      key: projectGroupIdentityKey(request.source_id, path, label),
      label,
      path,
    };
  };

  const threadByBindingInProject = new Map<string, Set<string>>();
  const recordBindingThreadLink = (
    projectKey: string,
    sourceId: string,
    threadUuid: string | null,
    bindingId: string | null
  ) => {
    if (!threadUuid || !bindingId) {
      return;
    }
    const key = JSON.stringify([projectKey, sourceId, bindingId]);
    let threads = threadByBindingInProject.get(key);
    if (!threads) {
      threads = new Set<string>();
      threadByBindingInProject.set(key, threads);
    }
    threads.add(threadUuid);
  };

  for (const job of scopedJobs) {
    const project = resolveJobProject(job);
    recordBindingThreadLink(
      project.key,
      job.source_id,
      trimOrNull(job.thread_uuid),
      trimOrNull(job.binding_id)
    );
  }
  for (const request of scopedRequests) {
    const project = resolveRequestProject(request);
    recordBindingThreadLink(
      project.key,
      request.source_id,
      trimOrNull(request.thread_uuid),
      trimOrNull(request.binding_id)
    );
  }

  const jobSessionIdentityOverrides = new Map<
    string,
    { threadUuid: string | null; bindingId: string | null; threadLabel: string | null }
  >();
  for (const request of scopedRequests) {
    const jobId = trimOrNull(request.job_id);
    if (!jobId) {
      continue;
    }
    const lookupKey = projectJobLookupKey(request.source_id, jobId);
    const existing = jobSessionIdentityOverrides.get(lookupKey) ?? {
      threadUuid: null,
      bindingId: null,
      threadLabel: null,
    };
    existing.threadUuid = existing.threadUuid ?? trimOrNull(request.thread_uuid);
    existing.bindingId = existing.bindingId ?? trimOrNull(request.binding_id);
    existing.threadLabel =
      existing.threadLabel ?? usableChatLabel(request.thread_label);
    jobSessionIdentityOverrides.set(lookupKey, existing);
  }

  const resolveCanonicalThreadAndBinding = (
    projectKey: string,
    sourceId: string,
    rawThreadUuid: string | null,
    rawBindingId: string | null
  ): { threadUuid: string | null; bindingId: string | null } => {
    let threadUuid = rawThreadUuid;
    const bindingId = rawBindingId;
    if (threadUuid === null && bindingId !== null) {
      const linkedThreads = threadByBindingInProject.get(
        JSON.stringify([projectKey, sourceId, bindingId])
      );
      if (linkedThreads && linkedThreads.size === 1) {
        threadUuid = Array.from(linkedThreads)[0] ?? null;
      }
    }
    return { threadUuid, bindingId };
  };

  const sessionAccumulators = new Map<string, SessionAccumulator>();

  const getOrCreateSession = (
    project: ResolvedProjectIdentity,
    sourceId: string,
    threadUuid: string | null,
    bindingId: string | null,
    isolatedKind: 'job' | 'request' | null,
    isolatedId: string | null
  ): SessionAccumulator => {
    const key =
      threadUuid !== null
        ? JSON.stringify(['session-thread', project.key, sourceId, threadUuid])
        : bindingId !== null
          ? JSON.stringify(['session-binding', project.key, sourceId, bindingId])
          : JSON.stringify([
              `session-${isolatedKind ?? 'request'}`,
              project.key,
              sourceId,
              isolatedId ?? 'unknown',
            ]);
    let session = sessionAccumulators.get(key);
    if (!session) {
      session = {
        key,
        sourceId,
        project,
        threadUuid,
        bindingId,
        isolatedKind: threadUuid === null && bindingId === null ? isolatedKind : null,
        isolatedId: threadUuid === null && bindingId === null ? isolatedId : null,
        threadLabels: [],
        jobs: [],
        requests: [],
      };
      sessionAccumulators.set(key, session);
    } else {
      if (session.threadUuid === null && threadUuid !== null) {
        session.threadUuid = threadUuid;
      }
      if (session.bindingId === null && bindingId !== null) {
        session.bindingId = bindingId;
      }
    }
    return session;
  };

  for (const job of scopedJobs) {
    const project = resolveJobProject(job);
    const lookupKey = projectJobLookupKey(job.source_id, job.id);
    const override = jobSessionIdentityOverrides.get(lookupKey);
    const canonical = resolveCanonicalThreadAndBinding(
      project.key,
      job.source_id,
      trimOrNull(job.thread_uuid) ?? override?.threadUuid ?? null,
      trimOrNull(job.binding_id) ?? override?.bindingId ?? null
    );
    const session = getOrCreateSession(
      project,
      job.source_id,
      canonical.threadUuid,
      canonical.bindingId,
      'job',
      job.id
    );
    const chatLabel =
      usableChatLabel(job.thread_label) ?? override?.threadLabel ?? null;
    if (chatLabel && !session.threadLabels.includes(chatLabel)) {
      session.threadLabels.push(chatLabel);
    }
    session.jobs.push(job);
  }

  for (const request of scopedRequests) {
    const project = resolveRequestProject(request);
    const jobId = trimOrNull(request.job_id);
    const linkedJob = jobId
      ? allJobsByKey.get(projectJobLookupKey(request.source_id, jobId)) ?? null
      : null;
    const canonical = resolveCanonicalThreadAndBinding(
      project.key,
      request.source_id,
      trimOrNull(request.thread_uuid) ?? trimOrNull(linkedJob?.thread_uuid),
      trimOrNull(request.binding_id) ?? trimOrNull(linkedJob?.binding_id)
    );
    const isolatedKind: 'job' | 'request' = jobId !== null ? 'job' : 'request';
    const isolatedId = jobId !== null ? jobId : request.id;
    const session = getOrCreateSession(
      project,
      request.source_id,
      canonical.threadUuid,
      canonical.bindingId,
      isolatedKind,
      isolatedId
    );
    const chatLabel =
      usableChatLabel(request.thread_label) ??
      usableChatLabel(linkedJob?.thread_label);
    if (chatLabel && !session.threadLabels.includes(chatLabel)) {
      session.threadLabels.push(chatLabel);
    }
    session.requests.push(request);
  }

  const projectGroupsByKey = new Map<
    string,
    { key: string; label: string; path: string | null; sessions: ProjectSessionView[] }
  >();
  let hiddenSessionCount = 0;

  for (const accumulator of sessionAccumulators.values()) {
    const orderedJobs = accumulator.jobs
      .filter((job) => {
        if (scope !== null || isJobActive(job) || isJobAttention(job)) {
          return true;
        }
        const updatedMs = safeTimestampMs(job.updated_at_ms);
        return updatedMs !== null && updatedMs >= cutoffMs;
      })
      .sort(
        (left, right) =>
          compareCaseInsensitiveThenExact(left.title, right.title) ||
          compareText(left.id, right.id) ||
          compareText(left.source_id, right.source_id)
      );
    const jobViews = orderedJobs.map((job) => buildProjectJobView(job));

    const parentRequestIdsWithAgentChildRequest = new Set<string>();
    for (const request of accumulator.requests) {
      const parentId = trimOrNull(request.parent_request_id);
      if (!parentId) {
        continue;
      }
      const hasAgentWork =
        isProviderExecutor(request.executor) ||
        (request.children ?? []).some(
          (child) => !isLocalMcpExecutor(child.executor)
        );
      if (hasAgentWork) {
        parentRequestIdsWithAgentChildRequest.add(
          projectRequestLookupKey(request.source_id, parentId)
        );
      }
    }

    const callCandidatesByKey = new Map<string, CallCandidate>();
    const recentEvents: ProjectEventView[] = [];

    const upsertCallCandidate = (candidate: CallCandidate) => {
      const dedupKey = JSON.stringify([candidate.sourceId, candidate.callKeyId]);
      const existing = callCandidatesByKey.get(dedupKey);
      if (!existing || shouldReplaceCallCandidate(existing, candidate)) {
        callCandidatesByKey.set(dedupKey, candidate);
      }
    };

    for (const request of accumulator.requests) {
      const children = request.children ?? [];
      const localChildren = children.filter((child) =>
        isLocalMcpExecutor(child.executor)
      );
      const agentChildren = children.filter(
        (child) => !isLocalMcpExecutor(child.executor)
      );

      for (const localChild of localChildren) {
        recentEvents.push(buildEventFromLocalChild(request, localChild));
      }

      if (agentChildren.length > 0) {
        for (const agentChild of agentChildren) {
          upsertCallCandidate(
            buildCallCandidateFromChild(
              request.source_id,
              request,
              agentChild,
              nowMs
            )
          );
        }
        if (isLocalMcpExecutor(request.executor)) {
          recentEvents.push(buildEventFromRequest(request));
        }
        continue;
      }

      const requestLookupKey = projectRequestLookupKey(
        request.source_id,
        request.id
      );
      if (parentRequestIdsWithAgentChildRequest.has(requestLookupKey)) {
        if (isLocalMcpExecutor(request.executor)) {
          recentEvents.push(buildEventFromRequest(request));
        }
        continue;
      }

      if (isProviderExecutor(request.executor)) {
        upsertCallCandidate(buildCallCandidateFromRequest(request, nowMs));
      } else {
        recentEvents.push(buildEventFromRequest(request));
      }
    }

    const allCallCandidates = Array.from(callCandidatesByKey.values());
    const updatedAtMs = maxTimestampMs([
      ...accumulator.jobs.map((job) => job.updated_at_ms),
      ...accumulator.requests.map((req) => requestSafeMeaningfulTimestampMs(req)),
      ...allCallCandidates.map((candidate) => candidate.sortAtMs),
      ...recentEvents.map((event) => event.atMs),
    ]);

    const retainRegardlessOfAge =
      scope !== null ||
      shouldRetainSessionRegardlessOfWindow(
        accumulator,
        jobViews,
        allCallCandidates
      );
    const withinWindow = updatedAtMs !== null && updatedAtMs >= cutoffMs;

    if (!retainRegardlessOfAge && !withinWindow) {
      hiddenSessionCount += 1;
      continue;
    }

    const sortedCandidates = [...allCallCandidates].sort((left, right) => {
      const priorityDiff = callSortPriority(left) - callSortPriority(right);
      if (priorityDiff !== 0) {
        return priorityDiff;
      }
      const leftMs = left.sortAtMs;
      const rightMs = right.sortAtMs;
      if (leftMs !== rightMs) {
        if (leftMs === null) {
          return 1;
        }
        if (rightMs === null) {
          return -1;
        }
        return rightMs - leftMs;
      }
      return compareText(left.view.key, right.view.key);
    });

    const selectedCalls: ProjectCallView[] = [];
    let terminalCallsIncluded = 0;
    for (const candidate of sortedCandidates) {
      if (isTerminalLifecycleState(candidate.state)) {
        if (
          scope === null &&
          (candidate.sortAtMs === null || candidate.sortAtMs < cutoffMs)
        ) {
          continue;
        }
        if (terminalCallsIncluded >= MAX_TERMINAL_CALLS_PER_SESSION) {
          continue;
        }
        terminalCallsIncluded += 1;
      }
      selectedCalls.push(candidate.view);
    }

    const sortedRecent = [...recentEvents]
      .sort((left, right) => {
        if (left.atMs !== right.atMs) {
          if (left.atMs === null) {
            return 1;
          }
          if (right.atMs === null) {
            return -1;
          }
          return right.atMs - left.atMs;
        }
        return compareText(left.key, right.key);
      })
      .slice(0, MAX_RECENT_EVENTS_PER_SESSION);

    const runningCount = allCallCandidates.filter(
      (candidate) => candidate.view.running
    ).length;
    const settledJobKeys = new Set(
      accumulator.jobs
        .filter((job) => job.state === 'completed')
        .map((job) => projectJobLookupKey(job.source_id, job.id))
    );
    const settledRequests = accumulator.requests.filter(
      (request) =>
        request.job_id !== null &&
        settledJobKeys.has(projectJobLookupKey(request.source_id, request.job_id))
    );
    const settledRequestSet = new Set(settledRequests);
    const settledCallKeys = new Set<string>();
    for (const request of settledRequests) {
      settledCallKeys.add(JSON.stringify(['call', request.source_id, request.id]));
      for (const child of request.children ?? []) {
        settledCallKeys.add(JSON.stringify(['call', request.source_id, child.id]));
      }
    }
    const summaryRequests = accumulator.requests.filter((request) => {
      if (settledRequestSet.has(request)) {
        return false;
      }
      if (!isTerminalLifecycleState(request.state) || scope !== null) {
        return true;
      }
      const requestAtMs = requestSafeMeaningfulTimestampMs(request);
      return requestAtMs !== null && requestAtMs >= cutoffMs;
    });
    const summaryState = resolveSessionSummaryState(
      jobViews,
      selectedCalls.filter((call) => !settledCallKeys.has(call.key)),
      summaryRequests,
      runningCount
    );

    const primaryLabel =
      accumulator.threadLabels[0] ??
      (accumulator.threadUuid !== null
        ? `Chat ${compactStableIdentifier(accumulator.threadUuid)}`
        : accumulator.bindingId !== null
          ? `Chat ${compactStableIdentifier(accumulator.bindingId)}`
          : orderedJobs[0] && trimOrNull(orderedJobs[0].title)
            ? trimOrNull(orderedJobs[0].title)!
            : 'Unknown chat');

    const detailParts = [
      `Chat: ${primaryLabel}`,
      `Thread ID: ${accumulator.threadUuid ?? 'Not reported'}`,
      `Binding: ${accumulator.bindingId ?? 'Not reported'}`,
      `Source: ${accumulator.sourceId}`,
      accumulator.isolatedKind && accumulator.isolatedId
        ? `Isolated ${accumulator.isolatedKind}: ${accumulator.isolatedId}`
        : '',
    ].filter((part) => part.length > 0);

    const sessionView: ProjectSessionView = {
      key: accumulator.key,
      label: primaryLabel,
      detail: detailParts.join(' · '),
      threadUuid: accumulator.threadUuid,
      bindingId: accumulator.bindingId,
      updatedAtMs,
      stateLabel: summaryState.stateLabel,
      tone: summaryState.tone,
      runningCount,
      attention: summaryState.attention,
      jobs: jobViews,
      calls: selectedCalls,
      recent: sortedRecent,
    };

    let group = projectGroupsByKey.get(accumulator.project.key);
    if (!group) {
      group = {
        key: accumulator.project.key,
        label: accumulator.project.label,
        path: accumulator.project.path,
        sessions: [],
      };
      projectGroupsByKey.set(accumulator.project.key, group);
    }
    group.sessions.push(sessionView);
  }

  const projects: ProjectGroupView[] = Array.from(projectGroupsByKey.values())
    .map((group) => {
      const sessions = [...group.sessions].sort(
        (left, right) =>
          compareCaseInsensitiveThenExact(left.label, right.label) ||
          compareText(left.threadUuid ?? '', right.threadUuid ?? '') ||
          compareText(left.bindingId ?? '', right.bindingId ?? '') ||
          compareText(left.key, right.key)
      );
      const groupRunningCount = sessions.reduce(
        (total, session) => total + session.runningCount,
        0
      );
      return {
        key: group.key,
        label: group.label,
        path: group.path,
        sessions,
        runningCount: groupRunningCount,
      };
    })
    .sort(
      (left, right) =>
        compareCaseInsensitiveThenExact(left.label, right.label) ||
        compareText(left.path ?? '', right.path ?? '') ||
        compareText(left.key, right.key)
    );

  const runningCount = projects.reduce(
    (total, project) => total + project.runningCount,
    0
  );

  const unavailableSources = sources.filter(
    (source) => source.availability !== 'available'
  );
  const unavailableSourceCount = unavailableSources.length;

  const warnings: string[] = [];
  const pushUniqueWarning = (message: string) => {
    const cleaned = compactFeedText(message, 320);
    if (cleaned.length > 0 && !warnings.includes(cleaned)) {
      warnings.push(cleaned);
    }
  };

  for (const warning of snapshot.warnings ?? []) {
    pushUniqueWarning(warning);
  }
  for (const source of unavailableSources) {
    const label = trimOrNull(source.label) ?? source.id;
    const message = trimOrNull(source.message);
    pushUniqueWarning(
      `Source ${label} is unavailable${message ? `: ${message}` : '.'}`
    );
  }
  const hasMore = snapshot.has_more === true;
  const jobsHasMore = snapshot.jobs_has_more === true;
  if (jobsHasMore) {
    pushUniqueWarning(
      'More jobs exist than this bounded observation page shows (jobs_has_more).'
    );
  }
  if (hasMore) {
    pushUniqueWarning(
      'More request activity is available beyond this bounded observation page (has_more).'
    );
  }

  let emptyMessage: string | null = null;
  if (projects.length === 0) {
    if (scope !== null) {
      emptyMessage =
        'No observed activity or job details matched the selected job scope in this bounded snapshot.';
    } else if (hiddenSessionCount > 0) {
      emptyMessage = `No active or recent sessions in the last ${windowHours}h (${hiddenSessionCount} older completed or failed ${
        hiddenSessionCount === 1 ? 'session is' : 'sessions are'
      } hidden by the ${windowHours}h window).`;
    } else if (unavailableSourceCount > 0) {
      emptyMessage =
        'No observed sessions are available from reachable sources. Some observation sources are currently unavailable.';
    } else if (jobsHasMore || hasMore) {
      emptyMessage =
        'No sessions are shown on this bounded observation page, but more records exist outside the current limit.';
    } else {
      emptyMessage = 'No observed project sessions are recorded in this snapshot.';
    }
  }

  return {
    projects,
    runningCount,
    hiddenSessionCount,
    unavailableSourceCount,
    warnings,
    hasMore,
    jobsHasMore,
    emptyMessage,
  };
}
