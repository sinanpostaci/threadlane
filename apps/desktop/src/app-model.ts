import type { CodexChatTarget } from './app-contract.ts';
import type {
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationTask,
} from './observation/types.ts';
import {
  formatRelativeTime,
  isLocalMcpExecutor,
  type ActivityFeedScope,
  type LifecycleTone,
} from './pages/activity-feed-model.ts';
import {
  PROJECTS_ARCHIVE_STORAGE_KEY,
  readProjectsArchiveCutoff,
  writeProjectsArchiveCutoff,
  type ProjectsArchiveStorage,
  type ProjectsArchiveStorageReader,
  type ProjectsArchiveWriteResult,
} from './pages/projects-archive.ts';
import {
  projectProjects,
  type ProjectAttentionItemView,
  type ProjectCallView,
  type ProjectDiagnosticRowView,
  type ProjectGroupView,
  type ProjectJobView,
  type ProjectSessionView,
  type ProjectsProjection,
  type ProjectsWindowHours,
} from './pages/projects-model.ts';
import {
  projectTaskStatus,
  summarizeProjectTaskStatuses,
  type ProjectTaskStatusTone,
} from './pages/projects-task-status.ts';
import type { BrandKey } from './types.ts';

export {
  PROJECTS_ARCHIVE_STORAGE_KEY,
  readProjectsArchiveCutoff,
  writeProjectsArchiveCutoff,
  type ProjectsArchiveStorage,
  type ProjectsArchiveStorageReader,
  type ProjectsArchiveWriteResult,
  type ProjectsWindowHours,
};

export type JournalWindowHours = ProjectsWindowHours;

export type StatusDotTone =
  | 'working'
  | 'attention'
  | 'danger'
  | 'success'
  | 'neutral';

export type CodexActionPlacement = 'attention_banner' | 'header' | 'none';

export interface SidebarChatItem {
  key: string;
  sourceId: string;
  projectKey: string;
  projectLabel: string;
  label: string;
  detail: string;
  updatedAtMs: number | null;
  ageLabel: string;
  workingCount: number;
  needsHumanCount: number;
  hasWorking: boolean;
  hasNeedsHuman: boolean;
  workingBadgeLabel: string | null;
  needsHumanBadgeLabel: string | null;
  primaryDotTone: StatusDotTone;
  secondaryDotTone: StatusDotTone | null;
  statusSummaryLabel: string;
  outsideWindow: boolean;
  outsideWindowLabel: string | null;
  canArchive: boolean;
  threadUuid: string | null;
}

export interface SidebarProjectGroup {
  key: string;
  sourceId: string;
  label: string;
  projectPath: string | null;
  latestUpdatedAtMs: number | null;
  ageLabel: string;
  workingCount: number;
  needsHumanCount: number;
  hasWorking: boolean;
  hasNeedsHuman: boolean;
  workingBadgeLabel: string | null;
  needsHumanBadgeLabel: string | null;
  chats: SidebarChatItem[];
}

export interface JournalTaskRow {
  key: string;
  taskId: string;
  label: string;
  statusRaw: string;
  statusLabel: string;
  tone: StatusDotTone;
  taskTone: ProjectTaskStatusTone;
  isConcreteFailure: boolean;
}

export interface JournalCallRow {
  key: string;
  title: string;
  roleLabel: string;
  model: string;
  effort: string | null;
  brand: BrandKey;
  statusLabel: string;
  tone: StatusDotTone;
  running: boolean;
  startedAtMs: number | null;
  finishedAtMs: number | null;
  ageLabel: string;
  durationLabel: string;
  usageLabel: string;
  detail: string;
  isWorkerCallOnly: boolean;
  workerPendingNote: string | null;
}

export interface JournalJobSection {
  key: string;
  jobId: string;
  sourceId: string;
  title: string;
  summary: string | null;
  planLabel: string | null;
  stateRaw: ObservationJob['state'];
  statusLabel: string;
  tone: StatusDotTone;
  isAuthoritativeJobDone: boolean;
  workerFinishedPendingAcceptance: boolean;
  updatedAtMs: number | null;
  ageLabel: string;
  olderThanWindow: boolean;
  olderWindowLabel: string | null;
  attention: boolean;
  attentionReason: string | null;
  nextStep: string | null;
  taskSummary: string | null;
  tasks: JournalTaskRow[];
  technicalDetails: Record<string, string>;
}

export interface JournalNoiseItem {
  key: string;
  label: string;
  atMs: number | null;
  ageLabel: string;
  detail: string;
}

export interface JournalOtherActivityGroup {
  key: string;
  count: number;
  title: string;
  summaryLabel: string;
  latestTimestampMs: number | null;
  ageLabel: string;
  items: JournalNoiseItem[];
}

export interface JournalAttentionBanner {
  key: string;
  kind: ProjectAttentionItemView['kind'];
  title: string;
  stateLabel: string;
  reason: string;
  nextStep: string;
  tone: 'attention' | 'danger' | 'neutral';
  ageLabel: string;
  jobId: string | null;
  requestId: string | null;
  olderThanWindow: boolean;
  showOpenInCodex: boolean;
}

export interface SelectedChatJournal {
  chatKey: string;
  sourceId: string;
  projectKey: string;
  projectLabel: string;
  projectPath: string | null;
  chatLabel: string;
  chatDetail: string;
  statusLabel: string;
  primaryTone: StatusDotTone;
  workingCount: number;
  needsHumanCount: number;
  workingBadgeLabel: string | null;
  needsHumanBadgeLabel: string | null;
  updatedAtMs: number | null;
  ageLabel: string;
  olderJobCount: number;
  outsideWindowNotice: string | null;
  canArchiveSession: boolean;
  attentionBanner: JournalAttentionBanner | null;
  additionalAttentionItems: ProjectAttentionItemView[];
  codexTarget: CodexChatTarget | null;
  codexActionPlacement: CodexActionPlacement;
  showHeaderCodexAction: boolean;
  jobs: JournalJobSection[];
  calls: JournalCallRow[];
  otherActivity: JournalOtherActivityGroup | null;
  diagnostics: ProjectDiagnosticRowView[];
}

export interface DesktopWorkspaceProjection {
  status: 'loading' | 'unavailable' | 'empty' | 'ready';
  unavailableReason: string | null;
  emptyMessage: string | null;
  isStale: boolean;
  staleReason: string | null;
  windowHours: JournalWindowHours;
  archiveBeforeMs: number | null;
  hiddenSessionCount: number;
  unavailableSourceCount: number;
  canArchiveHistory: boolean;
  canRestoreArchived: boolean;
  totalWorkingCount: number;
  totalNeedsHumanCount: number;
  projects: SidebarProjectGroup[];
  selectedChatKey: string | null;
  selectedJournal: SelectedChatJournal | null;
  hasMoreHistory: boolean;
  jobsHasMore: boolean;
  warnings: string[];
  diagnostics: ProjectDiagnosticRowView[];
}

export interface ProjectDesktopWorkspaceOptions {
  nowMs?: number;
  windowHours?: JournalWindowHours;
  archiveBeforeMs?: number | null;
  selectedChatKey?: string | null;
  scope?: ActivityFeedScope | null;
  loading?: boolean;
  error?: string | null;
}

const CANONICAL_UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isCanonicalUuid(value: string | null | undefined): value is string {
  if (typeof value !== 'string') {
    return false;
  }
  return CANONICAL_UUID_RE.test(value.trim());
}

function parseSessionSourceId(sessionKey: string): string {
  try {
    const parsed = JSON.parse(sessionKey) as unknown;
    if (Array.isArray(parsed) && typeof parsed[2] === 'string') {
      return parsed[2];
    }
  } catch {
    // Ignore malformed key fallback.
  }
  return '';
}

function parseProjectSourceId(projectKey: string, sessions: readonly ProjectSessionView[]): string {
  for (const session of sessions) {
    const sourceId = resolveSessionSourceId(session);
    if (sourceId) {
      return sourceId;
    }
  }
  const nulIndex = projectKey.indexOf('\u0000');
  if (nulIndex > 0) {
    return projectKey.slice(0, nulIndex);
  }
  return projectKey;
}

export function resolveSessionSourceId(session: ProjectSessionView): string {
  if (session.jobs.length > 0 && session.jobs[0]!.job.source_id) {
    return session.jobs[0]!.job.source_id;
  }
  if (session.attentionItems.length > 0 && session.attentionItems[0]!.sourceId) {
    return session.attentionItems[0]!.sourceId;
  }
  return parseSessionSourceId(session.key);
}

function mapLifecycleToneToDotTone(tone: LifecycleTone): StatusDotTone {
  switch (tone) {
    case 'active':
      return 'working';
    case 'waiting':
    case 'review':
      return 'attention';
    case 'danger':
      return 'danger';
    case 'ok':
      return 'success';
    case 'cancelled':
    case 'neutral':
    case 'unknown':
      return 'neutral';
  }
}

function mapTaskToneToDotTone(tone: ProjectTaskStatusTone): StatusDotTone {
  switch (tone) {
    case 'active':
      return 'working';
    case 'waiting':
      return 'attention';
    case 'danger':
      return 'danger';
    case 'ok':
      return 'success';
    case 'cancelled':
    case 'neutral':
    case 'unknown':
      return 'neutral';
  }
}

function formatAge(atMs: number | null, nowMs: number): string {
  if (atMs === null || !Number.isFinite(atMs)) {
    return 'Time not reported';
  }
  return formatRelativeTime(atMs, nowMs);
}

function formatWorkingBadge(count: number): string | null {
  if (count <= 0) {
    return null;
  }
  return `${count} working`;
}

function formatNeedsHumanBadge(count: number): string | null {
  if (count <= 0) {
    return null;
  }
  return count === 1 ? '1 needs you' : `${count} need you`;
}

function countSessionWorking(session: ProjectSessionView): number {
  const runningJobs = session.jobs.filter((jv) => {
    const state = jv.job.state;
    return (
      state === 'running' ||
      state === 'reviewing' ||
      jv.job.active_call_count > 0
    );
  }).length;
  return Math.max(session.runningCount, runningJobs);
}

function countSessionNeedsHuman(session: ProjectSessionView): number {
  // A failure is a result, not evidence of a pending human-input request.
  return session.attentionItems.length;
}

function isWorkerRoleLabel(role: string): boolean {
  const lower = role.trim().toLowerCase();
  return (
    lower === 'worker' ||
    lower === 'coder' ||
    lower === 'implementer'
  );
}

function isCompletedCallView(call: ProjectCallView): boolean {
  const lower = call.stateLabel.trim().toLowerCase();
  return (
    call.tone === 'ok' ||
    lower === 'completed' ||
    lower === 'done' ||
    lower === 'succeeded'
  );
}

function hasConcreteFailureToken(raw: string | null | undefined): boolean {
  if (typeof raw !== 'string') {
    return false;
  }
  const lower = raw.trim().toLowerCase();
  return (
    lower.includes('fail') ||
    lower.includes('timed_out') ||
    lower.includes('timeout') ||
    lower.includes('timed out') ||
    lower.includes('error') ||
    lower.includes('crash') ||
    lower.includes('abort') ||
    lower.includes('reject')
  );
}

function projectTaskRow(
  sourceId: string,
  jobId: string,
  task: ObservationTask,
  index: number
): JournalTaskRow {
  const projected = projectTaskStatus(task.status);
  const rawLower = (task.status ?? '').trim().toLowerCase();
  const isConcreteFailure =
    projected.tone === 'danger' || hasConcreteFailureToken(rawLower);

  let statusLabel = projected.label;
  let taskTone: ProjectTaskStatusTone = projected.tone;

  if (projected.tone === 'unknown' && isConcreteFailure) {
    statusLabel =
      rawLower.includes('time') ? 'Timed out' : 'Failed';
    taskTone = 'danger';
  }

  return {
    key: `${sourceId}\u0000${jobId}\u0000task\u0000${task.id || index}`,
    taskId: task.id,
    label: task.title.trim() || 'Unnamed task',
    statusRaw: task.status,
    statusLabel,
    tone: isConcreteFailure ? 'danger' : mapTaskToneToDotTone(taskTone),
    taskTone,
    isConcreteFailure,
  };
}

function projectJobSection(
  sourceId: string,
  jobView: ProjectJobView,
  sessionCalls: readonly ProjectCallView[],
  nowMs: number,
  windowHours: JournalWindowHours
): JournalJobSection {
  const rawJob = jobView.job;
  const tasks = rawJob.tasks.map((task, idx) =>
    projectTaskRow(sourceId, rawJob.id, task, idx)
  );
  const taskSummary =
    summarizeProjectTaskStatuses(rawJob.tasks) ??
    (jobView.taskSummary.trim() || null);

  const isAuthoritativeJobDone = rawJob.state === 'completed';
  const hasFinishedWorkerCall = sessionCalls.some(
    (c) => isWorkerRoleLabel(c.role) && isCompletedCallView(c)
  );
  const workerFinishedPendingAcceptance =
    !isAuthoritativeJobDone && hasFinishedWorkerCall;

  const hasFailedTask = tasks.some((t) => t.isConcreteFailure);
  const executionFailed = hasConcreteFailureToken(rawJob.execution_status);

  let statusLabel = jobView.stateLabel;
  let tone: StatusDotTone = 'neutral';

  if (isAuthoritativeJobDone) {
    statusLabel = 'Done';
    tone = 'success';
  } else if (
    rawJob.state === 'failed' ||
    executionFailed ||
    hasFailedTask
  ) {
    const timedOut =
      rawJob.execution_status.toLowerCase().includes('time') ||
      tasks.some((t) => t.statusLabel === 'Timed out');
    if (
      statusLabel.toLowerCase().includes('unknown') ||
      statusLabel.toLowerCase().includes('check') ||
      statusLabel.toLowerCase() === 'completed' ||
      statusLabel.toLowerCase() === 'done'
    ) {
      statusLabel = timedOut ? 'Timed out' : 'Failed';
    }
    tone = 'danger';
  } else if (
    rawJob.state === 'running' ||
    rawJob.state === 'reviewing' ||
    rawJob.active_call_count > 0
  ) {
    tone = 'working';
    if (
      statusLabel.toLowerCase() === 'completed' ||
      statusLabel.toLowerCase() === 'done'
    ) {
      statusLabel =
        rawJob.state === 'reviewing' ? 'Reviewing' : 'In progress';
    }
  } else if (jobView.attention || rawJob.state === 'needs_user') {
    tone = 'attention';
  } else if (workerFinishedPendingAcceptance) {
    tone = 'attention';
    if (
      statusLabel.toLowerCase() === 'completed' ||
      statusLabel.toLowerCase() === 'done'
    ) {
      statusLabel = 'Worker finished · awaiting acceptance';
    }
  }

  const ageLabel = formatAge(jobView.updatedAtMs, nowMs);
  const olderWindowLabel = jobView.olderThanWindow
    ? `Outside ${windowHours}h window · ${ageLabel}`
    : null;

  const technicalDetails: Record<string, string> = {
    'Job ID': rawJob.id,
    'Source ID': rawJob.source_id,
    State: rawJob.state,
    'Approval status': rawJob.approval_status || 'Not reported',
    'Execution status': rawJob.execution_status || 'Not reported',
  };
  if (rawJob.review_state) {
    technicalDetails['Review state'] = rawJob.review_state;
  }
  if (rawJob.plan_id) {
    technicalDetails['Plan ID'] =
      rawJob.plan_revision !== null
        ? `${rawJob.plan_id} (rev ${rawJob.plan_revision})`
        : rawJob.plan_id;
  }
  if (rawJob.thread_uuid) {
    technicalDetails['Thread UUID'] = rawJob.thread_uuid;
  }
  if (rawJob.binding_id) {
    technicalDetails['Binding ID'] = rawJob.binding_id;
  }

  return {
    key: jobView.key,
    jobId: rawJob.id,
    sourceId: rawJob.source_id,
    title: rawJob.title.trim() || 'Untitled job',
    summary: rawJob.summary?.trim() || null,
    planLabel: jobView.planLabel,
    stateRaw: rawJob.state,
    statusLabel,
    tone,
    isAuthoritativeJobDone,
    workerFinishedPendingAcceptance,
    updatedAtMs: jobView.updatedAtMs,
    ageLabel,
    olderThanWindow: jobView.olderThanWindow,
    olderWindowLabel,
    attention: jobView.attention,
    attentionReason: jobView.reason,
    nextStep: jobView.nextStep,
    taskSummary,
    tasks,
    technicalDetails,
  };
}

function projectCallRow(
  call: ProjectCallView,
  allJobsAcceptedDone: boolean,
  hasAnyJob: boolean,
  nowMs: number
): JournalCallRow {
  const workerRole = isWorkerRoleLabel(call.role);
  const callCompleted = isCompletedCallView(call);
  const isWorkerCallOnly =
    workerRole && callCompleted && (!hasAnyJob || !allJobsAcceptedDone);

  let statusLabel = call.stateLabel;
  let tone = mapLifecycleToneToDotTone(call.tone);
  let workerPendingNote: string | null = null;

  if (call.running) {
    tone = 'working';
  } else if (isWorkerCallOnly) {
    statusLabel = 'Worker call finished';
    tone = 'neutral';
    workerPendingNote = hasAnyJob
      ? 'Worker call finished; job Done requires authoritative job completion.'
      : 'Worker call finished; no accepted job completion recorded.';
  } else if (
    statusLabel.toLowerCase().includes('unknown') &&
    hasConcreteFailureToken(call.detail)
  ) {
    statusLabel = call.detail.toLowerCase().includes('time')
      ? 'Timed out'
      : 'Failed';
    tone = 'danger';
  }

  const ts = call.finishedAtMs ?? call.startedAtMs;
  const usageRaw = call.usageLabel.trim();
  const usageLabel =
    !usageRaw || usageRaw.toLowerCase() === 'not reported'
      ? 'Usage unknown'
      : usageRaw;

  return {
    key: call.key,
    title: call.title,
    roleLabel: call.role,
    model: call.model,
    effort: call.effort,
    brand: call.brand,
    statusLabel,
    tone,
    running: call.running,
    startedAtMs: call.startedAtMs,
    finishedAtMs: call.finishedAtMs,
    ageLabel: formatAge(ts, nowMs),
    durationLabel: call.durationLabel,
    usageLabel,
    detail: call.detail,
    isWorkerCallOnly,
    workerPendingNote,
  };
}

export function resolveSessionCodexChatTarget(
  session: ProjectSessionView,
  rawSnapshot: ObservationSnapshot | null
): CodexChatTarget | null {
  const sourceId = resolveSessionSourceId(session);
  if (!sourceId || !rawSnapshot) {
    return null;
  }

  const jobsById = new Map<string, ObservationJob>();
  for (const job of rawSnapshot.jobs) {
    if (job.source_id === sourceId) {
      jobsById.set(job.id, job);
    }
  }

  const requestsById = new Map<string, ObservationRequest>();
  for (const req of rawSnapshot.requests) {
    if (req.source_id === sourceId) {
      requestsById.set(req.id, req);
    }
  }

  // 1. Prefer the primary attention item's own observed job or request within the same source.
  for (const item of session.attentionItems) {
    if (item.sourceId !== sourceId) {
      continue;
    }
    if (item.jobId) {
      const job = jobsById.get(item.jobId);
      if (job && isCanonicalUuid(job.thread_uuid)) {
        return {
          source_id: sourceId,
          thread_uuid: job.thread_uuid.trim(),
          job_id: job.id,
        };
      }
    }
    if (item.requestId) {
      const req = requestsById.get(item.requestId);
      if (req && isCanonicalUuid(req.thread_uuid)) {
        return {
          source_id: sourceId,
          thread_uuid: req.thread_uuid.trim(),
          job_id: req.job_id ?? null,
        };
      }
    }
  }

  // 2. Check jobs in this session in order, scoped to the same source.
  for (const jobView of session.jobs) {
    const job = jobView.job;
    if (job.source_id !== sourceId) {
      continue;
    }
    if (isCanonicalUuid(job.thread_uuid)) {
      return {
        source_id: sourceId,
        thread_uuid: job.thread_uuid.trim(),
        job_id: job.id,
      };
    }
    for (const req of rawSnapshot.requests) {
      if (
        req.source_id === sourceId &&
        req.job_id === job.id &&
        isCanonicalUuid(req.thread_uuid)
      ) {
        return {
          source_id: sourceId,
          thread_uuid: req.thread_uuid.trim(),
          job_id: job.id,
        };
      }
    }
  }

  // 3. Check requests matching this session within the same source.
  if (isCanonicalUuid(session.threadUuid)) {
    const targetUuid = session.threadUuid.trim();
    const matchingRequest = rawSnapshot.requests.find(
      (req) =>
        req.source_id === sourceId &&
        isCanonicalUuid(req.thread_uuid) &&
        req.thread_uuid.trim().toLowerCase() === targetUuid.toLowerCase()
    );
    if (matchingRequest) {
      return {
        source_id: sourceId,
        thread_uuid: matchingRequest.thread_uuid!.trim(),
        job_id: matchingRequest.job_id ?? session.jobs[0]?.job.id ?? null,
      };
    }
    return {
      source_id: sourceId,
      thread_uuid: targetUuid,
      job_id: session.jobs[0]?.job.id ?? null,
    };
  }

  return null;
}

function buildSidebarChatItem(
  project: ProjectGroupView,
  session: ProjectSessionView,
  nowMs: number,
  windowHours: JournalWindowHours
): SidebarChatItem {
  const sourceId = resolveSessionSourceId(session);
  const workingCount = countSessionWorking(session);
  const needsHumanCount = countSessionNeedsHuman(session);
  const hasWorking = workingCount > 0;
  const hasNeedsHuman = needsHumanCount > 0;
  const workingBadgeLabel = formatWorkingBadge(workingCount);
  const needsHumanBadgeLabel = formatNeedsHumanBadge(needsHumanCount);

  const hasNonCompletedJob = session.jobs.some(
    (jv) => jv.job.state !== 'completed'
  );
  const hasWorkerOnlyCall =
    session.calls.some(
      (c) => isWorkerRoleLabel(c.role) && isCompletedCallView(c)
    ) &&
    (session.jobs.length === 0 || hasNonCompletedJob);

  let primaryDotTone: StatusDotTone = mapLifecycleToneToDotTone(session.tone);
  let secondaryDotTone: StatusDotTone | null = null;
  let statusSummaryLabel = session.stateLabel;

  if (hasNeedsHuman && hasWorking) {
    primaryDotTone = 'attention';
    secondaryDotTone = 'working';
    statusSummaryLabel = `${needsHumanBadgeLabel} · ${workingBadgeLabel}`;
  } else if (hasNeedsHuman) {
    primaryDotTone =
      session.tone === 'danger' ? 'danger' : 'attention';
    statusSummaryLabel = session.stateLabel || needsHumanBadgeLabel || 'Needs you';
  } else if (hasWorking) {
    primaryDotTone = 'working';
    statusSummaryLabel = workingBadgeLabel ?? 'Working';
  } else if (
    (statusSummaryLabel.toLowerCase() === 'completed' ||
      statusSummaryLabel.toLowerCase() === 'done') &&
    hasWorkerOnlyCall
  ) {
    primaryDotTone = 'neutral';
    statusSummaryLabel = 'Worker call finished';
  }

  const outsideWindow =
    session.olderJobCount > 0 ||
    session.attentionItems.some((item) => item.olderThanWindow);
  const ageLabel = formatAge(session.updatedAtMs, nowMs);
  const outsideWindowLabel = outsideWindow
    ? `Outside ${windowHours}h window · ${ageLabel}`
    : null;

  return {
    key: session.key,
    sourceId,
    projectKey: project.key,
    projectLabel: project.label,
    label: session.label,
    detail: session.detail,
    updatedAtMs: session.updatedAtMs,
    ageLabel,
    workingCount,
    needsHumanCount,
    hasWorking,
    hasNeedsHuman,
    workingBadgeLabel,
    needsHumanBadgeLabel,
    primaryDotTone,
    secondaryDotTone,
    statusSummaryLabel,
    outsideWindow,
    outsideWindowLabel,
    canArchive:
      !hasWorking && session.updatedAtMs !== null,
    threadUuid: session.threadUuid,
  };
}

function compareSidebarChats(left: SidebarChatItem, right: SidebarChatItem): number {
  const leftActive = left.hasWorking || left.hasNeedsHuman;
  const rightActive = right.hasWorking || right.hasNeedsHuman;
  if (leftActive !== rightActive) {
    return leftActive ? -1 : 1;
  }
  const leftTime = left.updatedAtMs ?? -1;
  const rightTime = right.updatedAtMs ?? -1;
  if (leftTime !== rightTime) {
    return rightTime - leftTime;
  }
  if (left.hasNeedsHuman !== right.hasNeedsHuman) {
    return left.hasNeedsHuman ? -1 : 1;
  }
  if (left.hasWorking !== right.hasWorking) {
    return left.hasWorking ? -1 : 1;
  }
  const byLabel = left.label.localeCompare(right.label);
  if (byLabel !== 0) {
    return byLabel;
  }
  return left.key.localeCompare(right.key);
}

function compareSidebarProjects(
  left: SidebarProjectGroup,
  right: SidebarProjectGroup
): number {
  const leftActive = left.hasWorking || left.hasNeedsHuman;
  const rightActive = right.hasWorking || right.hasNeedsHuman;
  if (leftActive !== rightActive) {
    return leftActive ? -1 : 1;
  }
  const leftTime = left.latestUpdatedAtMs ?? -1;
  const rightTime = right.latestUpdatedAtMs ?? -1;
  if (leftTime !== rightTime) {
    return rightTime - leftTime;
  }
  const byLabel = left.label.localeCompare(right.label);
  if (byLabel !== 0) {
    return byLabel;
  }
  return left.key.localeCompare(right.key);
}

function collectSessionNoiseItems(
  session: ProjectSessionView,
  sourceId: string,
  rawSnapshot: ObservationSnapshot | null,
  nowMs: number
): JournalNoiseItem[] {
  const itemsByKey = new Map<string, JournalNoiseItem>();

  for (const event of session.recent) {
    itemsByKey.set(event.key, {
      key: event.key,
      label: event.label,
      atMs: event.atMs,
      ageLabel: formatAge(event.atMs, nowMs),
      detail: event.detail,
    });
  }

  // Also include any additional low-level MCP reads from the raw snapshot belonging to this session
  // that were truncated by MAX_RECENT_EVENTS_PER_SESSION in projectProjects.
  if (rawSnapshot && sourceId) {
    const sessionJobIds = new Set(session.jobs.map((jv) => jv.job.id));
    for (const req of rawSnapshot.requests) {
      if (req.source_id !== sourceId) {
        continue;
      }
      const matchesThread =
        session.threadUuid !== null &&
        req.thread_uuid !== null &&
        req.thread_uuid.trim().toLowerCase() === session.threadUuid.trim().toLowerCase();
      const matchesBinding =
        session.threadUuid === null &&
        session.bindingId !== null &&
        req.binding_id === session.bindingId;
      const matchesJob = req.job_id !== null && sessionJobIds.has(req.job_id);

      if (!matchesThread && !matchesBinding && !matchesJob) {
        continue;
      }

      if (isLocalMcpExecutor(req.executor)) {
        const key = JSON.stringify(['event', req.source_id, req.id]);
        if (!itemsByKey.has(key)) {
          const atMs = req.updated_at_ms ?? req.finished_at_ms ?? req.started_at_ms;
          itemsByKey.set(key, {
            key,
            label: req.title.trim() || req.operation.trim() || 'MCP read',
            atMs,
            ageLabel: formatAge(atMs, nowMs),
            detail: req.summary?.trim() || req.operation,
          });
        }
      }
    }
  }

  const items = Array.from(itemsByKey.values());
  items.sort((left, right) => {
    const lt = left.atMs ?? -1;
    const rt = right.atMs ?? -1;
    if (lt !== rt) {
      return rt - lt;
    }
    return left.key.localeCompare(right.key);
  });
  return items;
}

function projectSelectedChatJournal(
  project: ProjectGroupView,
  session: ProjectSessionView,
  rawSnapshot: ObservationSnapshot | null,
  nowMs: number,
  windowHours: JournalWindowHours,
  globalDiagnostics: ProjectDiagnosticRowView[]
): SelectedChatJournal {
  const sourceId = resolveSessionSourceId(session);
  const jobs = session.jobs.map((jobView) =>
    projectJobSection(sourceId, jobView, session.calls, nowMs, windowHours)
  );

  const hasAnyJob = jobs.length > 0;
  const allJobsAcceptedDone =
    hasAnyJob && jobs.every((j) => j.isAuthoritativeJobDone);

  const calls = session.calls.map((call) =>
    projectCallRow(call, allJobsAcceptedDone, hasAnyJob, nowMs)
  );

  const noiseItems = collectSessionNoiseItems(
    session,
    sourceId,
    rawSnapshot,
    nowMs
  );
  const latestNoiseMs =
    noiseItems.length > 0 ? (noiseItems[0]?.atMs ?? null) : null;

  const otherActivity: JournalOtherActivityGroup | null =
    noiseItems.length > 0
      ? {
          key: `${session.key}\u0000other-activity`,
          count: noiseItems.length,
          title: 'Other activity',
          summaryLabel:
            noiseItems.length === 1
              ? '1 low-level MCP read / status check'
              : `${noiseItems.length} low-level MCP reads & status checks`,
          latestTimestampMs: latestNoiseMs,
          ageLabel: formatAge(latestNoiseMs, nowMs),
          items: noiseItems,
        }
      : null;

  const codexTarget = resolveSessionCodexChatTarget(session, rawSnapshot);
  const primaryAttention = session.attentionItems[0] ?? null;

  let attentionBanner: JournalAttentionBanner | null = null;
  if (primaryAttention) {
    const bannerTone: JournalAttentionBanner['tone'] =
      session.tone === 'danger'
        ? 'danger'
        : primaryAttention.kind === 'status_check'
          ? 'neutral'
          : 'attention';
    attentionBanner = {
      key: primaryAttention.key,
      kind: primaryAttention.kind,
      title: primaryAttention.title,
      stateLabel: primaryAttention.stateLabel,
      reason: primaryAttention.reason,
      nextStep: primaryAttention.nextStep,
      tone: bannerTone,
      ageLabel: formatAge(primaryAttention.updatedAtMs, nowMs),
      jobId: primaryAttention.jobId,
      requestId: primaryAttention.requestId,
      olderThanWindow: primaryAttention.olderThanWindow,
      showOpenInCodex: codexTarget !== null,
    };
  }

  const codexActionPlacement: CodexActionPlacement =
    codexTarget === null
      ? 'none'
      : attentionBanner !== null
        ? 'attention_banner'
        : 'header';

  const workingCount = countSessionWorking(session);
  const needsHumanCount = countSessionNeedsHuman(session);
  const hasWorking = workingCount > 0;
  const hasNeedsHuman = needsHumanCount > 0;

  let statusLabel = session.stateLabel;
  let primaryTone = mapLifecycleToneToDotTone(session.tone);

  if (hasNeedsHuman && hasWorking) {
    statusLabel = `${formatNeedsHumanBadge(needsHumanCount)} · ${formatWorkingBadge(workingCount)}`;
    primaryTone = 'attention';
  } else if (
    (statusLabel.toLowerCase() === 'completed' ||
      statusLabel.toLowerCase() === 'done') &&
    (!hasAnyJob || !allJobsAcceptedDone)
  ) {
    const firstNonDone = jobs.find((j) => !j.isAuthoritativeJobDone);
    statusLabel = firstNonDone
      ? firstNonDone.statusLabel
      : 'Worker call finished';
    primaryTone = firstNonDone ? firstNonDone.tone : 'neutral';
  }

  const ageLabel = formatAge(session.updatedAtMs, nowMs);
  const outsideWindowNotice =
    session.olderJobCount > 0 ||
    session.attentionItems.some((item) => item.olderThanWindow)
      ? `Retained unresolved work from outside the ${windowHours}h window (${ageLabel}).`
      : null;

  const sessionDiagnostics = globalDiagnostics.filter(
    (d) => !d.sourceId || d.sourceId === sourceId
  );

  return {
    chatKey: session.key,
    sourceId,
    projectKey: project.key,
    projectLabel: project.label,
    projectPath: project.path,
    chatLabel: session.label,
    chatDetail: session.detail,
    statusLabel,
    primaryTone,
    workingCount,
    needsHumanCount,
    workingBadgeLabel: formatWorkingBadge(workingCount),
    needsHumanBadgeLabel: formatNeedsHumanBadge(needsHumanCount),
    updatedAtMs: session.updatedAtMs,
    ageLabel,
    olderJobCount: session.olderJobCount,
    outsideWindowNotice,
    canArchiveSession:
      !hasWorking && session.updatedAtMs !== null,
    attentionBanner,
    additionalAttentionItems: session.attentionItems.slice(1),
    codexTarget,
    codexActionPlacement,
    showHeaderCodexAction: codexActionPlacement === 'header',
    jobs,
    calls,
    otherActivity,
    diagnostics: sessionDiagnostics,
  };
}

export function projectDesktopWorkspace(
  snapshot: ObservationSnapshot | null,
  options: ProjectDesktopWorkspaceOptions = {}
): DesktopWorkspaceProjection {
  const nowMs = options.nowMs ?? Date.now();
  const windowHours: JournalWindowHours = options.windowHours === 24 ? 24 : 6;
  const archiveBeforeMs = options.archiveBeforeMs ?? null;
  const loading = Boolean(options.loading);
  const error = options.error?.trim() ? options.error.trim() : null;

  if (snapshot === null) {
    if (loading) {
      return {
        status: 'loading',
        unavailableReason: null,
        emptyMessage: null,
        isStale: false,
        staleReason: null,
        windowHours,
        archiveBeforeMs,
        hiddenSessionCount: 0,
        unavailableSourceCount: 0,
        canArchiveHistory: false,
        canRestoreArchived: archiveBeforeMs !== null,
        totalWorkingCount: 0,
        totalNeedsHumanCount: 0,
        projects: [],
        selectedChatKey: options.selectedChatKey ?? null,
        selectedJournal: null,
        hasMoreHistory: false,
        jobsHasMore: false,
        warnings: [],
        diagnostics: [],
      };
    }

    return {
      status: 'unavailable',
      unavailableReason:
        error ?? 'Observation runtime is not connected or returned no snapshot.',
      emptyMessage: null,
      isStale: false,
      staleReason: null,
      windowHours,
      archiveBeforeMs,
      hiddenSessionCount: 0,
      unavailableSourceCount: 0,
      canArchiveHistory: false,
      canRestoreArchived: archiveBeforeMs !== null,
      totalWorkingCount: 0,
      totalNeedsHumanCount: 0,
      projects: [],
      selectedChatKey: options.selectedChatKey ?? null,
      selectedJournal: null,
      hasMoreHistory: false,
      jobsHasMore: false,
      warnings: [],
      diagnostics: [],
    };
  }

  const baseProjection: ProjectsProjection = projectProjects(snapshot, {
    nowMs,
    windowHours,
    scope: options.scope ?? null,
    archiveBeforeMs,
  });

  const sessionLookup = new Map<
    string,
    { project: ProjectGroupView; session: ProjectSessionView }
  >();

  const sidebarProjects: SidebarProjectGroup[] = [];
  let totalWorkingCount = 0;
  let totalNeedsHumanCount = 0;
  let canArchiveHistory = false;

  for (const proj of baseProjection.projects) {
    const chats = proj.sessions
      .map((session) => buildSidebarChatItem(proj, session, nowMs, windowHours))
      .sort(compareSidebarChats);

    for (const session of proj.sessions) {
      sessionLookup.set(session.key, { project: proj, session });
    }

    let latestUpdatedAtMs: number | null = null;
    let projectWorkingCount = 0;
    let projectNeedsHumanCount = 0;

    for (const chat of chats) {
      projectWorkingCount += chat.workingCount;
      projectNeedsHumanCount += chat.needsHumanCount;
      if (chat.canArchive) {
        canArchiveHistory = true;
      }
      if (
        chat.updatedAtMs !== null &&
        (latestUpdatedAtMs === null || chat.updatedAtMs > latestUpdatedAtMs)
      ) {
        latestUpdatedAtMs = chat.updatedAtMs;
      }
    }

    totalWorkingCount += projectWorkingCount;
    totalNeedsHumanCount += projectNeedsHumanCount;

    sidebarProjects.push({
      key: proj.key,
      sourceId: parseProjectSourceId(proj.key, proj.sessions),
      label: proj.label,
      projectPath: proj.path,
      latestUpdatedAtMs,
      ageLabel: formatAge(latestUpdatedAtMs, nowMs),
      workingCount: projectWorkingCount,
      needsHumanCount: projectNeedsHumanCount,
      hasWorking: projectWorkingCount > 0,
      hasNeedsHuman: projectNeedsHumanCount > 0,
      workingBadgeLabel: formatWorkingBadge(projectWorkingCount),
      needsHumanBadgeLabel: formatNeedsHumanBadge(projectNeedsHumanCount),
      chats,
    });
  }

  sidebarProjects.sort(compareSidebarProjects);

  const allSortedChats = sidebarProjects.flatMap((group) => group.chats);
  const requestedChatKey = options.selectedChatKey ?? null;

  let resolvedChatKey: string | null = null;
  if (requestedChatKey !== null && sessionLookup.has(requestedChatKey)) {
    resolvedChatKey = requestedChatKey;
  } else if (allSortedChats.length > 0) {
    resolvedChatKey = allSortedChats[0]!.key;
  } else {
    resolvedChatKey = requestedChatKey;
  }

  const selectedPair =
    resolvedChatKey !== null ? (sessionLookup.get(resolvedChatKey) ?? null) : null;

  const selectedJournal = selectedPair
    ? projectSelectedChatJournal(
        selectedPair.project,
        selectedPair.session,
        snapshot,
        nowMs,
        windowHours,
        baseProjection.diagnostics
      )
    : null;

  const allSourcesUnavailable =
    snapshot.sources.length > 0 &&
    snapshot.sources.every((s) => s.availability !== 'available') &&
    allSortedChats.length === 0;

  if (allSourcesUnavailable) {
    const firstUnavailableMsg =
      snapshot.sources.find((s) => s.message?.trim())?.message?.trim() ??
      error ??
      'Linked observation sources are currently unavailable.';
    return {
      status: 'unavailable',
      unavailableReason: firstUnavailableMsg,
      emptyMessage: null,
      isStale: Boolean(error),
      staleReason: error,
      windowHours,
      archiveBeforeMs,
      hiddenSessionCount: baseProjection.hiddenSessionCount,
      unavailableSourceCount: baseProjection.unavailableSourceCount,
      canArchiveHistory: false,
      canRestoreArchived: archiveBeforeMs !== null,
      totalWorkingCount: 0,
      totalNeedsHumanCount: 0,
      projects: sidebarProjects,
      selectedChatKey: resolvedChatKey,
      selectedJournal: null,
      hasMoreHistory: baseProjection.hasMore,
      jobsHasMore: baseProjection.jobsHasMore,
      warnings: baseProjection.warnings,
      diagnostics: baseProjection.diagnostics,
    };
  }

  const status: DesktopWorkspaceProjection['status'] =
    allSortedChats.length === 0 && sidebarProjects.length === 0 ? 'empty' : 'ready';

  return {
    status,
    unavailableReason: null,
    emptyMessage:
      status === 'empty'
        ? (baseProjection.emptyMessage ??
            'No active or recent chats in this window.')
        : null,
    isStale: Boolean(error),
    staleReason: error,
    windowHours,
    archiveBeforeMs,
    hiddenSessionCount: baseProjection.hiddenSessionCount,
    unavailableSourceCount: baseProjection.unavailableSourceCount,
    canArchiveHistory,
    canRestoreArchived: archiveBeforeMs !== null,
    totalWorkingCount,
    totalNeedsHumanCount,
    projects: sidebarProjects,
    selectedChatKey: resolvedChatKey,
    selectedJournal,
    hasMoreHistory: baseProjection.hasMore,
    jobsHasMore: baseProjection.jobsHasMore,
    warnings: baseProjection.warnings,
    diagnostics: baseProjection.diagnostics,
  };
}
