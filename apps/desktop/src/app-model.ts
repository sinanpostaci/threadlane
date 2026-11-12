import type {
  CodexChatTarget,
  WorkspaceScopedChatTarget,
} from './app-contract.ts';
import type {
  ObservationActor,
  ObservationCall,
  ObservationDelivery,
  ObservationDeliveryStage,
  ObservationExecutor,
  ObservationJob,
  ObservationJobState,
  ObservationLifecycleState,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
  ObservationTask,
} from './observation/types.ts';
import {
  describeDeliveryStage,
  formatFriendlyModel,
  formatRecordedOperation,
  formatRelativeTime,
  hasExecutorRecord,
  isLocalMcpExecutor,
  projectDuration,
  projectUsage,
  resolveExecutorBrand,
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
import { projectDiagnostics } from './pages/projects-diagnostics.ts';
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
import {
  DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER,
  DEFAULT_WORKSPACE_CHAT_TAB,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_CHAT_LIFECYCLE_FILTERS,
  WORKSPACE_CHAT_SURFACE_TABS,
  WORKSPACE_SURFACE_SEPARATION,
  boundWorkspaceCompactTrayRows,
  createUnavailableConfiguredSelection,
  evaluateWorkspaceNativeSampleAt,
  formatWorkspaceChatIdentityKey,
  isValidWorkspaceChatIdentity,
  normalizeWorkspaceAcceptanceProofForJob,
  normalizeWorkspaceAcceptanceProofForTask,
  normalizeWorkspaceChatTab,
  normalizeWorkspaceSnapshot,
  parseWorkspaceSnapshot,
  readWorkspaceChatArchivePreferences,
  regenerateWorkspaceSnapshotAt,
  resolveWorkspaceChatLifecycle,
  sanitizeWorkspaceLabel,
  sanitizeWorkspaceSummary,
  sanitizeWorkspaceSummaryWithMeta,
  setWorkspaceChatArchivePreference,
  upgradeLegacyObservationSnapshot,
  utf8ByteLength,
  validateCorrelationId,
  validateWorkspaceChatIdentity,
  workspaceChatIdentitiesEqual,
  writeWorkspaceChatArchivePreferences,
} from './workspace-contract.ts';
import type {
  WorkspaceAcceptanceProof,
  WorkspaceAcceptanceStatus,
  WorkspaceActivityState,
  WorkspaceAttentionKind,
  WorkspaceAttentionState,
  WorkspaceAvailabilityState,
  WorkspaceChatArchiveEntry,
  WorkspaceChatArchivePreferenceMap,
  WorkspaceChatIdentity,
  WorkspaceChatLifecycle,
  WorkspaceChatPreferenceStorage,
  WorkspaceChatSummary,
  WorkspaceChatSurfaceTab,
  WorkspaceCombinedCandidate,
  WorkspaceCompactTrayRow,
  WorkspaceConfiguredSelection,
  WorkspaceConnectionState,
  WorkspaceEvaluatedNativeSample,
  WorkspaceFreshnessState,
  WorkspaceHistoricalFailure,
  WorkspaceJobHistory,
  WorkspaceJobHistoryTruncation,
  WorkspaceJobResult,
  WorkspaceJobSummary,
  WorkspaceMessageCompleteness,
  WorkspaceMessageItem,
  WorkspaceMessagesWindow,
  WorkspaceNativeSample,
  WorkspaceOutboxRecord,
  WorkspaceRecordLinks,
  WorkspaceRunRecord,
  WorkspaceSafeMessageSourceKind,
  WorkspaceSnapshot,
  WorkspaceSnapshotBounds,
  WorkspaceSurfaceSeparationContract,
  WorkspaceTaskAttempt,
  WorkspaceTaskCandidate,
  WorkspaceTaskHistory,
} from './workspace-contract.ts';

export {
  DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER,
  DEFAULT_WORKSPACE_CHAT_TAB,
  PROJECTS_ARCHIVE_STORAGE_KEY,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_CHAT_LIFECYCLE_FILTERS,
  WORKSPACE_CHAT_SURFACE_TABS,
  WORKSPACE_SURFACE_SEPARATION,
  boundWorkspaceCompactTrayRows,
  evaluateWorkspaceNativeSampleAt,
  formatWorkspaceChatIdentityKey,
  isValidWorkspaceChatIdentity,
  normalizeWorkspaceAcceptanceProofForJob,
  normalizeWorkspaceAcceptanceProofForTask,
  normalizeWorkspaceChatTab,
  normalizeWorkspaceSnapshot,
  parseWorkspaceSnapshot,
  readProjectsArchiveCutoff,
  readWorkspaceChatArchivePreferences,
  regenerateWorkspaceSnapshotAt,
  resolveWorkspaceChatLifecycle,
  sanitizeWorkspaceLabel,
  sanitizeWorkspaceSummary,
  sanitizeWorkspaceSummaryWithMeta,
  setWorkspaceChatArchivePreference,
  upgradeLegacyObservationSnapshot,
  validateWorkspaceChatIdentity,
  workspaceChatIdentitiesEqual,
  writeProjectsArchiveCutoff,
  writeWorkspaceChatArchivePreferences,
  type ProjectsArchiveStorage,
  type ProjectsArchiveStorageReader,
  type ProjectsArchiveWriteResult,
  type ProjectsWindowHours,
  type WorkspaceAcceptanceProof,
  type WorkspaceAcceptanceStatus,
  type WorkspaceActivityState,
  type WorkspaceAttentionKind,
  type WorkspaceAttentionState,
  type WorkspaceAvailabilityState,
  type WorkspaceChatArchiveEntry,
  type WorkspaceChatArchivePreferenceMap,
  type WorkspaceChatIdentity,
  type WorkspaceChatLifecycle,
  type WorkspaceChatPreferenceStorage,
  type WorkspaceChatSummary,
  type WorkspaceChatSurfaceTab,
  type WorkspaceCombinedCandidate,
  type WorkspaceCompactTrayRow,
  type WorkspaceConfiguredSelection,
  type WorkspaceConnectionState,
  type WorkspaceEvaluatedNativeSample,
  type WorkspaceFreshnessState,
  type WorkspaceHistoricalFailure,
  type WorkspaceJobHistory,
  type WorkspaceJobHistoryTruncation,
  type WorkspaceJobResult,
  type WorkspaceJobSummary,
  type WorkspaceMessageCompleteness,
  type WorkspaceMessageItem,
  type WorkspaceMessagesWindow,
  type WorkspaceNativeSample,
  type WorkspaceOutboxRecord,
  type WorkspaceRecordLinks,
  type WorkspaceRunRecord,
  type WorkspaceSafeMessageSourceKind,
  type WorkspaceScopedChatTarget,
  type WorkspaceSnapshot,
  type WorkspaceSnapshotBounds,
  type WorkspaceSurfaceSeparationContract,
  type WorkspaceTaskAttempt,
  type WorkspaceTaskCandidate,
  type WorkspaceTaskHistory,
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

// ---------------------------------------------------------------------------
// Canonical Four-Part Chat Workspace Production Projection (`WorkspaceSnapshot`)
// ---------------------------------------------------------------------------

export interface WorkspaceUnavailableSourceView {
  source: ObservationSource;
  sourceId: string;
  sourceLabel: string;
  projectPath: string | null;
  rawMessage: string | null;
  statusText: string;
}

export interface WorkspaceNativeChatStateView {
  availability: WorkspaceAvailabilityState;
  activity: WorkspaceActivityState;
  freshness: WorkspaceFreshnessState;
  connectionState: WorkspaceConnectionState;
  connectionRoute: string | null;
  connectionDiagnostic: string | null;
  sampleAgeMs: number | null;
  staleThresholdMs: number;
  observedAtMs: number | null;
  lastSuccessAtMs: number | null;
  availabilityLabel: string;
  activityLabel: string;
  tone: StatusDotTone;
  statusNotice: string | null;
}

export interface WorkspaceConfiguredSelectionView {
  selectedModel: string | null;
  selectedEffort: string | null;
  evidence: WorkspaceConfiguredSelection['evidence'];
  countsAsRuntimeTelemetry: false;
  isStale: boolean;
  displayLabel: string;
  friendlyModelLabel: string | null;
  statusNote: string;
}

export interface WorkspaceWorkerSummaryView {
  activeWorkerCount: number;
  completedWorkerRunCount: number;
  failedWorkerRunCount: number;
  hasActiveWorkers: boolean;
  badgeLabel: string | null;
  statusLabel: string;
}

export interface WorkspaceChatJobSummaryView {
  totalJobCount: number;
  activeJobCount: number;
  verifiedAcceptedJobCount: number;
  pendingAcceptanceJobCount: number;
  failedJobCount: number;
  needsAttentionJobCount: number;
  statusLabel: string;
}

export interface WorkspaceChatBlockerView {
  key: string;
  origin:
    | 'native_chat'
    | 'job_attention'
    | 'task_attention'
    | 'job_needs_user'
    | 'request_waiting_for_user';
  kind: WorkspaceAttentionKind;
  kindLabel: string;
  reason: string;
  nextStep: string | null;
  jobId: string | null;
  taskId: string | null;
  requestId: string | null;
  questionId: string | null;
  blockingHoldIds: string[];
  blockingOutboxIds: string[];
  updatedAtMs: number | null;
  ageLabel: string;
}

export interface WorkspaceArchivedBlockerSummary {
  chatKey: string;
  identity: WorkspaceChatIdentity;
  threadLabel: string;
  projectLabel: string;
  needsHumanCount: number;
  primaryReason: string;
  nextStep: string | null;
  updatedAtMs: number | null;
}

export interface WorkspaceChatSidebarItem {
  chatKey: string;
  identity: WorkspaceChatIdentity;
  lifecycle: WorkspaceChatLifecycle;
  projectKey: string;
  projectLabel: string;
  canonicalProjectScope: string;
  threadLabel: string;
  updatedAtMs: number | null;
  ageLabel: string;
  nativeState: WorkspaceNativeChatStateView;
  configuredSelection: WorkspaceConfiguredSelectionView;
  workerSummary: WorkspaceWorkerSummaryView;
  jobSummary: WorkspaceChatJobSummaryView;
  needsHumanCount: number;
  hasCurrentBlocker: boolean;
  needsHumanBadgeLabel: string | null;
  blockers: WorkspaceChatBlockerView[];
  primaryDotTone: StatusDotTone;
  statusSummaryLabel: string;
  canArchive: boolean;
  canRestore: boolean;
  scopedTarget: WorkspaceScopedChatTarget;
  codexTarget: CodexChatTarget;
}

export interface WorkspaceProjectGroupView {
  projectKey: string;
  sourceId: string;
  canonicalProjectScope: string;
  label: string;
  lifecycle: WorkspaceChatLifecycle;
  latestUpdatedAtMs: number | null;
  ageLabel: string;
  chatCount: number;
  activeWorkerCount: number;
  workingCoworkerCount: number;
  needsHumanCount: number;
  hasActiveWork: boolean;
  hasNeedsHuman: boolean;
  chats: WorkspaceChatSidebarItem[];
}

export interface WorkspaceMessageItemView {
  itemId: string;
  sourceKind: WorkspaceSafeMessageSourceKind;
  sourceKindLabel: string;
  sourceRecordId: string;
  recordedAtMs: number | null;
  ageLabel: string;
  sourceSeq: number;
  subIndex: number;
  actorRole: string | null;
  jobId: string | null;
  taskId: string | null;
  runId: string | null;
  attemptNo: number | null;
  summary: string;
  completeness: WorkspaceMessageCompleteness;
  isIncomplete: boolean;
  completenessNotices: string[];
}

export interface WorkspaceChatMessagesSurface {
  kind: 'safe_summaries_results_questions';
  availability: WorkspaceAvailabilityState;
  availabilityLabel: string;
  items: WorkspaceMessageItemView[];
  maxItems: number;
  omittedCount: number;
  hasMore: boolean;
  truncated: boolean;
  unscopedWithheldCount: number;
  requiresTranscript: false;
  allowsSyntheticUserMessages: false;
  emptyMessage: string | null;
  statusNotices: string[];
}

export interface WorkspaceHistoricalFailureView {
  failureId: string | null;
  stage: string;
  failedAtMs: number | null;
  ageLabel: string;
  taskId: string | null;
  attemptNo: number | null;
  runId: string | null;
  outboxId: string | null;
  reason: string;
  supersededOrResolved: boolean;
  statusLabel: string;
}

export interface WorkspaceAttentionView {
  kind: WorkspaceAttentionKind;
  kindLabel: string;
  reason: string;
  nextStep: string | null;
  blockingHoldIds: string[];
  blockingOutboxIds: string[];
  questionId: string | null;
  updatedAtMs: number | null;
  ageLabel: string;
}

export interface WorkspaceTaskAttemptView {
  attemptNo: number;
  isCurrentAttempt: boolean;
  statusRaw: string;
  statusLabel: string;
  tone: StatusDotTone;
  runId: string | null;
  workerId: string | null;
  outboxId: string | null;
  executor: ObservationExecutor | null;
  startedAtMs: number | null;
  finishedAtMs: number | null;
  ageLabel: string;
  summary: string | null;
}

export interface WorkspaceTaskCandidateView {
  candidateId: string;
  attemptNo: number;
  workerId: string | null;
  headCommitSha: string | null;
  treeSha: string | null;
  verificationPassed: boolean | null;
  verificationLabel: string;
  reviewStatusRaw: string;
  reviewVerdictId: string | null;
  reviewReceiptId: string | null;
  producedAtMs: number | null;
  ageLabel: string;
  summary: string | null;
  isActiveCandidate: boolean;
  isAcceptedByVerifiedProof: boolean;
}

export interface WorkspaceTaskView {
  taskId: string;
  title: string;
  role: string | null;
  difficulty: string | null;
  stage: string | null;
  statusRaw: string;
  statusLabel: string;
  tone: StatusDotTone;
  dependsOn: string[];
  currentAttemptNo: number;
  currentAttempt: WorkspaceTaskAttemptView | null;
  pastAttempts: WorkspaceTaskAttemptView[];
  allAttempts: WorkspaceTaskAttemptView[];
  currentAttemptNotice: string | null;
  candidates: WorkspaceTaskCandidateView[];
  activeCandidateId: string | null;
  recordedAcceptedCandidateId: string | null;
  historicalFailure: WorkspaceHistoricalFailureView | null;
  currentAttention: WorkspaceAttentionView | null;
  acceptanceProof: WorkspaceAcceptanceProof;
  isVerifiedAccepted: boolean;
  acceptanceLabel: string;
}

export interface WorkspaceCombinedCandidateView {
  combinedCandidateId: string;
  jobRevision: number | null;
  statusRaw: string;
  finalCommitSha: string | null;
  finalTreeSha: string | null;
  candidateDigest: string | null;
  closureReservationId: string | null;
  checkCount: number;
  producedAtMs: number | null;
  ageLabel: string;
  isAcceptedByVerifiedProof: boolean;
}

export interface WorkspaceJobResultView {
  outcome: string;
  summary: string;
  producedAtMs: number | null;
  ageLabel: string;
  deliveryStage: ObservationDeliveryStage;
  deliveryStageLabel: string;
  deliveryStageNote: string;
  closureVerdictId: string | null;
}

export interface WorkspaceJobView {
  jobKey: string;
  sourceId: string;
  jobId: string;
  bindingId: string | null;
  chatIdentity: WorkspaceChatIdentity;
  title: string;
  summary: string | null;
  jobRevision: number | null;
  stateRaw: ObservationJobState;
  phase: string | null;
  approvalStatus: string;
  executionStatus: string;
  reviewState: string | null;
  statusLabel: string;
  tone: StatusDotTone;
  createdAtMs: number | null;
  updatedAtMs: number | null;
  ageLabel: string;
  tasks: WorkspaceTaskView[];
  runs: WorkspaceRunRecord[];
  outbox: WorkspaceOutboxRecord[];
  combinedCandidates: WorkspaceCombinedCandidateView[];
  result: WorkspaceJobResultView | null;
  links: WorkspaceRecordLinks;
  historicalFailure: WorkspaceHistoricalFailureView | null;
  currentAttention: WorkspaceAttentionView | null;
  acceptanceProof: WorkspaceAcceptanceProof;
  isVerifiedAccepted: boolean;
  acceptanceLabel: string;
  truncation: WorkspaceJobHistoryTruncation;
  isTruncated: boolean;
  truncationNotices: string[];
}

export interface WorkspaceChatJobsSurface {
  kind: 'durable_job_task_attempt_candidate_ledger';
  items: WorkspaceJobView[];
  totalJobCount: number;
  activeJobCount: number;
  verifiedAcceptedJobCount: number;
  historicalFailureJobCount: number;
  currentAttentionJobCount: number;
  jobHistoriesOmitted: number;
  jobsHasMore: boolean;
  jobsCapped: boolean;
  unscopedWithheldCount: number;
  emptyMessage: string | null;
  statusNotices: string[];
}

export interface WorkspaceActivityChildCallView {
  call: ObservationCall;
  callId: string;
  title: string;
  stateRaw: ObservationLifecycleState;
  stateLabel: string;
  tone: StatusDotTone;
  running: boolean;
  isLocalMcpCall: boolean;
  executorLabel: string;
  brand: BrandKey;
  durationLabel: string;
  usageLabel: string;
  summary: string | null;
}

export interface WorkspaceActivityDeliveryView {
  record: ObservationDelivery;
  stage: ObservationDeliveryStage;
  stageLabel: string;
  stageNote: string;
  atMs: number | null;
  ageLabel: string;
  recipient: string | null;
  summary: string;
}

export interface WorkspaceActivityItemView {
  requestKey: string;
  sourceId: string;
  requestId: string;
  jobId: string | null;
  parentRequestId: string | null;
  chatIdentity: WorkspaceChatIdentity;
  title: string;
  operationRaw: string;
  operationLabel: string;
  stateRaw: ObservationLifecycleState;
  stateLabel: string;
  tone: StatusDotTone;
  running: boolean;
  isLocalMcpCall: boolean;
  completesChat: false;
  statusNote: string | null;
  actor: ObservationActor;
  historicalActorSelectedModel: string | null;
  executor: ObservationExecutor | null;
  executorLabel: string;
  executorModelLabel: string | null;
  brand: BrandKey;
  startedAtMs: number | null;
  finishedAtMs: number | null;
  updatedAtMs: number;
  ageLabel: string;
  durationLabel: string;
  usageLabel: string;
  summary: string | null;
  deliveries: WorkspaceActivityDeliveryView[];
  children: WorkspaceActivityChildCallView[];
  reportedChildCount: number;
  omittedChildCallCount: number;
  truncationNotice: string | null;
}

export interface WorkspaceChatActivitySurface {
  kind: 'paginated_request_call_delivery_audit_feed';
  items: WorkspaceActivityItemView[];
  totalScopedCount: number;
  activeJobFilter: string | null;
  runningCount: number;
  hasMore: boolean;
  requestsCapped: boolean;
  nextCursor: string | null;
  unscopedWithheldCount: number;
  emptyMessage: string | null;
  statusNotices: string[];
}

export interface WorkspaceSelectedChatProjection {
  chatKey: string;
  identity: WorkspaceChatIdentity;
  lifecycle: WorkspaceChatLifecycle;
  projectKey: string;
  projectLabel: string;
  canonicalProjectScope: string;
  threadLabel: string;
  updatedAtMs: number | null;
  ageLabel: string;
  activeTab: WorkspaceChatSurfaceTab;
  availableTabs: readonly WorkspaceChatSurfaceTab[];
  nativeState: WorkspaceNativeChatStateView;
  configuredSelection: WorkspaceConfiguredSelectionView;
  workerSummary: WorkspaceWorkerSummaryView;
  jobSummary: WorkspaceChatJobSummaryView;
  needsHumanCount: number;
  hasCurrentBlocker: boolean;
  blockers: WorkspaceChatBlockerView[];
  primaryBlocker: WorkspaceChatBlockerView | null;
  scopedTarget: WorkspaceScopedChatTarget;
  codexTarget: CodexChatTarget;
  statusNotices: string[];
  diagnostics: ProjectDiagnosticRowView[];
  messages: WorkspaceChatMessagesSurface;
  jobs: WorkspaceChatJobsSurface;
  activity: WorkspaceChatActivitySurface;
}

export interface ProjectWorkspaceSnapshotOptions {
  nowMs?: number;
  lifecycleFilter?: WorkspaceChatLifecycle;
  archivePreferences?: WorkspaceChatArchivePreferenceMap;
  preferenceStorage?: Pick<WorkspaceChatPreferenceStorage, 'getItem'> | null;
  selectedChatKey?: string | null;
  selectedTab?: WorkspaceChatSurfaceTab | string | null;
  activityJobId?: string | null;
  loading?: boolean;
  error?: string | null;
}

export interface WorkspaceProductionProjection {
  status: 'loading' | 'unavailable' | 'empty' | 'ready';
  unavailableReason: string | null;
  emptyMessage: string | null;
  isStale: boolean;
  staleReason: string | null;
  referenceNowMs: number;
  lifecycleFilter: WorkspaceChatLifecycle;
  surfaceSeparation: WorkspaceSurfaceSeparationContract;
  availableTabs: readonly WorkspaceChatSurfaceTab[];
  defaultTab: typeof DEFAULT_WORKSPACE_CHAT_TAB;
  activeProjects: WorkspaceProjectGroupView[];
  archivedProjects: WorkspaceProjectGroupView[];
  visibleProjects: WorkspaceProjectGroupView[];
  activeChatCount: number;
  archivedChatCount: number;
  totalWorkingCoworkerCount: number;
  totalActiveWorkerCount: number;
  totalNeedsHumanCount: number;
  activeNeedsHumanCount: number;
  archivedNeedsHumanCount: number;
  archivedBlockerCount: number;
  archivedBlockedChats: WorkspaceArchivedBlockerSummary[];
  archivedBlockerNotice: string | null;
  unavailableSourceCount: number;
  unavailableSources: WorkspaceUnavailableSourceView[];
  bounds: WorkspaceSnapshotBounds;
  boundsNotices: string[];
  statusNotices: string[];
  selectedChatKey: string | null;
  selectedChat: WorkspaceSelectedChatProjection | null;
  warnings: string[];
  diagnostics: ProjectDiagnosticRowView[];
}

const WORKSPACE_UTF8_ENCODER = new TextEncoder();

function compareUtf8Lexical(left: string, right: string): number {
  if (left === right) {
    return 0;
  }
  const leftBytes = WORKSPACE_UTF8_ENCODER.encode(left);
  const rightBytes = WORKSPACE_UTF8_ENCODER.encode(right);
  const minLen = Math.min(leftBytes.byteLength, rightBytes.byteLength);
  for (let i = 0; i < minLen; i += 1) {
    const diff = (leftBytes[i] ?? 0) - (rightBytes[i] ?? 0);
    if (diff !== 0) {
      return diff;
    }
  }
  return leftBytes.byteLength - rightBytes.byteLength;
}

export function formatWorkspaceProjectGroupKey(
  sourceId: string,
  canonicalProjectScope: string,
): string {
  return [
    'wproj:v1',
    `${utf8ByteLength(sourceId)}:${sourceId}`,
    `${utf8ByteLength(canonicalProjectScope)}:${canonicalProjectScope}`,
  ].join(':');
}

function projectScopeBasename(canonicalProjectScope: string): string {
  const parts = canonicalProjectScope.split('/').filter((seg) => seg.length > 0);
  return parts.at(-1) ?? canonicalProjectScope;
}

function isDefaultChatPlaceholderLabel(label: string | null | undefined): boolean {
  if (typeof label !== 'string') {
    return true;
  }
  const normalized = label.trim().replace(/\s+/gu, ' ').toLowerCase();
  return normalized.length === 0 || normalized === 'codex chat';
}

function isDefaultProjectPlaceholderLabel(label: string | null | undefined): boolean {
  if (typeof label !== 'string') {
    return true;
  }
  const normalized = label.trim().replace(/\s+/gu, ' ').toLowerCase();
  return normalized.length === 0 || normalized === 'project';
}

function isLocalMcpCallExecutorOrOp(
  executor: ObservationExecutor | null | undefined,
  operation?: string | null,
): boolean {
  if (isLocalMcpExecutor(executor)) {
    return true;
  }
  const tool = executor?.tool?.trim() ?? '';
  if (/^threadlane_mcp$/i.test(tool)) {
    return true;
  }
  const op = operation?.trim() ?? '';
  if (!hasExecutorRecord(executor) && /^mcp\./i.test(op)) {
    return true;
  }
  return false;
}

function isLocalMcpRequestRecord(req: ObservationRequest): boolean {
  if (!isLocalMcpCallExecutorOrOp(req.executor, req.operation)) {
    return false;
  }
  return req.children.every((child) => isLocalMcpCallExecutorOrOp(child.executor, null));
}

function isActiveWorkerRunStatus(status: string | null | undefined): boolean {
  if (typeof status !== 'string') {
    return false;
  }
  const lower = status.trim().toLowerCase();
  return (
    lower === 'running' ||
    lower === 'in_progress' ||
    lower === 'active' ||
    lower === 'starting' ||
    lower === 'dispatched'
  );
}

function isCompletedWorkerRunStatus(status: string | null | undefined): boolean {
  if (typeof status !== 'string') {
    return false;
  }
  const lower = status.trim().toLowerCase();
  return lower === 'completed' || lower === 'succeeded' || lower === 'done' || lower === 'accepted';
}

function isActiveObservationJobState(state: ObservationJobState): boolean {
  return (
    state === 'planning' ||
    state === 'ready' ||
    state === 'running' ||
    state === 'reviewing' ||
    state === 'needs_user'
  );
}

function formatMillisecondsCompact(ms: number): string {
  const safe = Math.max(0, Math.trunc(ms));
  if (safe < 1_000) {
    return `${safe}ms`;
  }
  const seconds = Math.round((safe / 1_000) * 10) / 10;
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remSeconds = Math.round(seconds % 60);
  return remSeconds > 0 ? `${minutes}m ${remSeconds}s` : `${minutes}m`;
}

function formatSafeMessageSourceKindLabel(kind: WorkspaceSafeMessageSourceKind): string {
  switch (kind) {
    case 'work_brief_summary':
      return 'Work brief';
    case 'plan_summary':
      return 'Plan summary';
    case 'question_prompt':
      return 'Question';
    case 'question_answer':
      return 'Answer';
    case 'task_result_summary':
      return 'Task result';
    case 'review_verdict_summary':
      return 'Review verdict';
    case 'closure_summary':
      return 'Closure summary';
    case 'job_result_summary':
      return 'Job result';
    case 'attention_summary':
      return 'Attention';
    case 'journal_summary':
      return 'Journal summary';
  }
}

function formatWorkspaceAvailabilityLabel(state: WorkspaceAvailabilityState): string {
  switch (state) {
    case 'available':
      return 'Available';
    case 'stale':
      return 'Stale';
    case 'disconnected':
      return 'Disconnected';
    case 'unavailable':
      return 'Unavailable';
  }
}

function formatAttentionKindLabel(kind: WorkspaceAttentionKind): string {
  switch (kind) {
    case 'needs_approval':
      return 'Needs approval';
    case 'needs_user_input':
      return 'Needs user input';
    case 'on_hold':
      return 'On hold';
    case 'unresolved_effect':
      return 'Unresolved effect';
    case 'blocked_failure':
      return 'Blocked by failure';
    case 'review_required':
      return 'Review required';
    case 'none':
      return 'No attention needed';
    case 'unknown':
      return 'Needs attention';
  }
}

function formatObservationLifecycleLabel(state: ObservationLifecycleState): {
  label: string;
  tone: StatusDotTone;
} {
  switch (state) {
    case 'queued':
      return { label: 'Queued', tone: 'neutral' };
    case 'running':
      return { label: 'Running', tone: 'working' };
    case 'waiting_for_user':
      return { label: 'Waiting for user', tone: 'attention' };
    case 'completed':
      return { label: 'Completed', tone: 'success' };
    case 'ready_for_review':
      return { label: 'Ready for review', tone: 'attention' };
    case 'failed':
      return { label: 'Failed', tone: 'danger' };
    case 'cancelled':
      return { label: 'Cancelled', tone: 'neutral' };
    case 'unknown':
      return { label: 'Status unknown', tone: 'neutral' };
  }
}

function tryValidateChatIdentity(candidate: {
  source_id: unknown;
  binding_id: unknown;
  thread_uuid: unknown;
  canonical_project_scope: unknown;
}): WorkspaceChatIdentity | null {
  try {
    return validateWorkspaceChatIdentity(candidate);
  } catch {
    return null;
  }
}

interface KnownSnapshotScopes {
  scopesByTriple: Map<string, Set<string>>;
  scopesBySource: Map<string, Set<string>>;
}

function collectKnownScopes(ws: WorkspaceSnapshot): KnownSnapshotScopes {
  const scopesByTriple = new Map<string, Set<string>>();
  const scopesBySource = new Map<string, Set<string>>();

  const addSourceScope = (sourceId: string, scope: string) => {
    if (sourceId.length === 0 || scope.length === 0) {
      return;
    }
    let sourceSet = scopesBySource.get(sourceId);
    if (!sourceSet) {
      sourceSet = new Set<string>();
      scopesBySource.set(sourceId, sourceSet);
    }
    sourceSet.add(scope);
  };

  const recordIdentity = (identity: WorkspaceChatIdentity | null) => {
    if (identity === null || !isValidWorkspaceChatIdentity(identity)) {
      return;
    }
    const valid = validateWorkspaceChatIdentity(identity);
    const tripleKey = `${valid.source_id}\u0000${valid.binding_id}\u0000${valid.thread_uuid}`;
    let set = scopesByTriple.get(tripleKey);
    if (!set) {
      set = new Set<string>();
      scopesByTriple.set(tripleKey, set);
    }
    set.add(valid.canonical_project_scope);
    addSourceScope(valid.source_id, valid.canonical_project_scope);
  };

  for (const src of ws.observation.sources) {
    if (typeof src.id === 'string' && typeof src.project_path === 'string') {
      addSourceScope(src.id, src.project_path);
    }
  }
  for (const chat of ws.chats) {
    recordIdentity(chat.identity);
  }
  for (const sample of ws.native_samples) {
    recordIdentity(sample.chat_identity);
  }
  for (const history of ws.job_histories) {
    recordIdentity(history.job.chat_identity);
  }
  for (const item of ws.messages.items) {
    recordIdentity(item.scope.chat_identity);
  }
  for (const obsJob of ws.observation.jobs) {
    if (
      typeof obsJob.binding_id === 'string' &&
      typeof obsJob.thread_uuid === 'string' &&
      typeof obsJob.project_path === 'string'
    ) {
      recordIdentity(
        tryValidateChatIdentity({
          source_id: obsJob.source_id,
          binding_id: obsJob.binding_id,
          thread_uuid: obsJob.thread_uuid,
          canonical_project_scope: obsJob.project_path,
        }),
      );
    }
  }

  return { scopesByTriple, scopesBySource };
}

function resolveObservationRequestChatIdentityWithScopes(
  request: ObservationRequest,
  snapshot: WorkspaceSnapshot,
  knownScopes: KnownSnapshotScopes,
): WorkspaceChatIdentity | null {
  if (
    typeof request.source_id !== 'string' ||
    typeof request.binding_id !== 'string' ||
    typeof request.thread_uuid !== 'string'
  ) {
    return null;
  }

  const explicitScopeRaw = (
    request as ObservationRequest & { canonical_project_scope?: unknown }
  ).canonical_project_scope;
  const explicitScope =
    typeof explicitScopeRaw === 'string' && explicitScopeRaw.length > 0
      ? explicitScopeRaw
      : null;

  let linkedJobScope: string | null = null;
  if (request.job_id !== null) {
    const matchedHistory = snapshot.job_histories.find(
      (h) => h.job.source_id === request.source_id && h.job.job_id === request.job_id,
    );
    if (matchedHistory) {
      if (
        matchedHistory.job.binding_id !== null &&
        matchedHistory.job.binding_id !== request.binding_id
      ) {
        return null;
      }
      if (matchedHistory.job.chat_identity !== null) {
        if (
          matchedHistory.job.chat_identity.binding_id !== request.binding_id ||
          matchedHistory.job.chat_identity.thread_uuid !== request.thread_uuid
        ) {
          return null;
        }
        linkedJobScope = matchedHistory.job.chat_identity.canonical_project_scope;
      }
    }

    const matchedObsJob: ObservationJob | undefined = snapshot.observation.jobs.find(
      (j) => j.source_id === request.source_id && j.id === request.job_id,
    );
    if (matchedObsJob) {
      if (
        matchedObsJob.binding_id !== null &&
        matchedObsJob.binding_id !== request.binding_id
      ) {
        return null;
      }
      if (
        matchedObsJob.thread_uuid !== null &&
        matchedObsJob.thread_uuid !== request.thread_uuid
      ) {
        return null;
      }
      if (linkedJobScope === null && typeof matchedObsJob.project_path === 'string') {
        linkedJobScope = matchedObsJob.project_path;
      }
    }
  }

  if (
    explicitScope !== null &&
    linkedJobScope !== null &&
    explicitScope !== linkedJobScope
  ) {
    return null;
  }

  let resolvedScope: string | null = explicitScope ?? linkedJobScope;

  if (resolvedScope === null) {
    const tripleKey = `${request.source_id}\u0000${request.binding_id}\u0000${request.thread_uuid}`;
    const tripleScopes = knownScopes.scopesByTriple.get(tripleKey);
    if (tripleScopes && tripleScopes.size > 1) {
      // Multiple canonical project scopes share this triple; refuse to guess by label.
      return null;
    }
    if (tripleScopes && tripleScopes.size === 1) {
      resolvedScope = Array.from(tripleScopes)[0] ?? null;
    }
  }

  if (resolvedScope === null) {
    const sourceScopes = knownScopes.scopesBySource.get(request.source_id);
    if (sourceScopes && sourceScopes.size === 1) {
      resolvedScope = Array.from(sourceScopes)[0] ?? null;
    }
  }

  if (resolvedScope === null) {
    return null;
  }

  return tryValidateChatIdentity({
    source_id: request.source_id,
    binding_id: request.binding_id,
    thread_uuid: request.thread_uuid,
    canonical_project_scope: resolvedScope,
  });
}

/**
 * Resolves the verified four-part `WorkspaceChatIdentity` for an `ObservationRequest`
 * without ever falling back to `thread_label`, `project_label`, `sessionId`, or
 * substituting `job_id` for missing `binding_id` / `thread_uuid`.
 */
export function resolveObservationRequestChatIdentity(
  request: ObservationRequest,
  snapshot: WorkspaceSnapshot,
): WorkspaceChatIdentity | null {
  return resolveObservationRequestChatIdentityWithScopes(
    request,
    snapshot,
    collectKnownScopes(snapshot),
  );
}

function coerceToNormalizedWorkspaceSnapshot(
  input: WorkspaceSnapshot | ObservationSnapshot,
): WorkspaceSnapshot {
  if (
    'observation' in input &&
    typeof input.observation === 'object' &&
    input.observation !== null
  ) {
    return normalizeWorkspaceSnapshot(input);
  }
  return upgradeLegacyObservationSnapshot(input as ObservationSnapshot);
}

function projectNativeChatStateAndSelection(
  sample: WorkspaceNativeSample | null,
  chatSummary: WorkspaceChatSummary | null,
  referenceNowMs: number,
  snapshotGeneratedAtMs: number,
  staleThresholdMs: number,
): {
  nativeState: WorkspaceNativeChatStateView;
  configuredSelection: WorkspaceConfiguredSelectionView;
} {
  let availability: WorkspaceAvailabilityState = 'unavailable';
  let activity: WorkspaceActivityState = 'unknown';
  let freshness: WorkspaceFreshnessState = 'unknown';
  let connectionState: WorkspaceConnectionState = 'unavailable';
  let connectionRoute: string | null = null;
  let connectionDiagnostic: string | null = null;
  let sampleAgeMs: number | null = null;
  let observedAtMs: number | null = null;
  let lastSuccessAtMs: number | null = null;
  let effectiveSelection: WorkspaceConfiguredSelection = createUnavailableConfiguredSelection();

  if (sample !== null) {
    const evaluated = evaluateWorkspaceNativeSampleAt(
      sample,
      referenceNowMs,
      staleThresholdMs,
    );
    availability = evaluated.effective_availability;
    activity = evaluated.effective_activity;
    freshness = evaluated.freshness;
    sampleAgeMs = evaluated.sample_age_ms;
    effectiveSelection = evaluated.effective_configured_selection;
    observedAtMs = sample.observed_at_ms;
    lastSuccessAtMs = sample.last_success_at_ms;
    connectionRoute = sample.connection.route;
    connectionDiagnostic = sample.connection.diagnostic;

    if (availability === 'disconnected') {
      connectionState = 'disconnected';
    } else if (availability === 'stale' && sample.connection.state === 'connected') {
      connectionState = 'degraded';
    } else if (availability === 'unavailable' && sample.connection.state === 'connected') {
      connectionState = 'unavailable';
    } else {
      connectionState = sample.connection.state;
    }
  } else if (chatSummary !== null) {
    const baselineMs = chatSummary.updated_at_ms ?? (snapshotGeneratedAtMs > 0 ? snapshotGeneratedAtMs : null);
    const summaryExceededStaleWindow =
      chatSummary.availability === 'available' &&
      baselineMs !== null &&
      referenceNowMs - baselineMs > staleThresholdMs;

    if (summaryExceededStaleWindow) {
      availability = 'stale';
      activity = 'unknown';
      freshness = 'stale';
      sampleAgeMs = Math.max(0, Math.trunc(referenceNowMs) - baselineMs);
      connectionState = 'degraded';
      effectiveSelection = createUnavailableConfiguredSelection();
    } else {
      availability = chatSummary.availability;
      activity = chatSummary.availability === 'available' ? chatSummary.activity : 'unknown';
      freshness =
        chatSummary.availability === 'available'
          ? 'fresh'
          : chatSummary.availability === 'stale'
            ? 'stale'
            : 'unknown';
      connectionState =
        chatSummary.availability === 'available'
          ? 'connected'
          : chatSummary.availability === 'disconnected'
            ? 'disconnected'
            : chatSummary.availability === 'stale'
              ? 'degraded'
              : 'unavailable';
      effectiveSelection =
        chatSummary.availability === 'available'
          ? chatSummary.configured_selection
          : createUnavailableConfiguredSelection();
    }
  }

  let availabilityLabel: string;
  switch (availability) {
    case 'available':
      availabilityLabel = 'Live';
      break;
    case 'stale':
      availabilityLabel = 'Stale sample';
      break;
    case 'disconnected':
      availabilityLabel = 'Disconnected';
      break;
    case 'unavailable':
      availabilityLabel = 'Native state unavailable';
      break;
  }

  let activityLabel: string;
  let tone: StatusDotTone = 'neutral';
  if (availability === 'available') {
    switch (activity) {
      case 'working':
        activityLabel = 'Coworker working';
        tone = 'working';
        break;
      case 'idle':
        activityLabel = 'Coworker idle';
        tone = 'neutral';
        break;
      case 'waiting_for_approval':
        activityLabel = 'Coworker waiting for approval';
        tone = 'attention';
        break;
      case 'waiting_for_input':
        activityLabel = 'Coworker waiting for user input';
        tone = 'attention';
        break;
      case 'unknown':
        activityLabel = 'Coworker activity unknown';
        tone = 'neutral';
        break;
    }
  } else if (availability === 'stale') {
    activityLabel = 'Coworker state stale';
  } else if (availability === 'disconnected') {
    activityLabel = 'Coworker disconnected';
  } else {
    activityLabel = 'Coworker state unavailable';
  }

  const diagSuffix = connectionDiagnostic ? ` (${connectionDiagnostic})` : '';
  let statusNotice: string | null = null;
  if (availability === 'stale' || freshness === 'stale') {
    const ageSummary =
      sampleAgeMs !== null
        ? `${formatMillisecondsCompact(sampleAgeMs)} old (threshold ${formatMillisecondsCompact(staleThresholdMs)})`
        : `past the ${formatMillisecondsCompact(staleThresholdMs)} freshness threshold`;
    statusNotice = `Native chat sample is stale (${ageSummary}). Live Coworker activity and selected model are withheld until a fresh sample arrives.`;
  } else if (availability === 'disconnected') {
    statusNotice = `Native chat connection is disconnected${diagSuffix}. Live Coworker activity and selected model are unavailable.`;
  } else if (freshness === 'never_succeeded') {
    statusNotice = `Native chat sampler has not recorded a successful sample yet${diagSuffix}.`;
  } else if (availability === 'unavailable') {
    statusNotice =
      sample !== null
        ? `Native chat state is currently unavailable${diagSuffix}.`
        : 'Native chat state is unavailable; no live native sample is recorded for this chat.';
  }

  const isSelectionStale = availability === 'stale' || freshness === 'stale';
  let configuredSelection: WorkspaceConfiguredSelectionView;
  if (
    availability === 'available' &&
    effectiveSelection.evidence === 'verified_configuration' &&
    (effectiveSelection.selected_model !== null || effectiveSelection.selected_effort !== null)
  ) {
    const modelPart = effectiveSelection.selected_model;
    const effortPart = effectiveSelection.selected_effort;
    const displayLabel =
      modelPart !== null && effortPart !== null
        ? `${modelPart} · ${effortPart}`
        : modelPart !== null
          ? modelPart
          : `Effort: ${effortPart ?? 'unknown'}`;
    configuredSelection = {
      selectedModel: modelPart,
      selectedEffort: effortPart,
      evidence: 'verified_configuration',
      countsAsRuntimeTelemetry: false,
      isStale: false,
      displayLabel,
      friendlyModelLabel: modelPart !== null ? formatFriendlyModel(modelPart) : null,
      statusNote:
        'Verified configured chat selection (not per-turn runtime execution telemetry).',
    };
  } else if (isSelectionStale) {
    configuredSelection = {
      selectedModel: null,
      selectedEffort: null,
      evidence: 'unavailable',
      countsAsRuntimeTelemetry: false,
      isStale: true,
      displayLabel: 'Selected model stale',
      friendlyModelLabel: null,
      statusNote: 'Selected model withheld because the native chat sample is stale.',
    };
  } else {
    configuredSelection = {
      selectedModel: null,
      selectedEffort: null,
      evidence: 'unavailable',
      countsAsRuntimeTelemetry: false,
      isStale: false,
      displayLabel: 'Selected model unavailable',
      friendlyModelLabel: null,
      statusNote: 'Verified configured model selection is unavailable.',
    };
  }

  return {
    nativeState: {
      availability,
      activity,
      freshness,
      connectionState,
      connectionRoute,
      connectionDiagnostic,
      sampleAgeMs,
      staleThresholdMs,
      observedAtMs,
      lastSuccessAtMs,
      availabilityLabel,
      activityLabel,
      tone,
      statusNotice,
    },
    configuredSelection,
  };
}

function projectHistoricalFailureView(
  failure: WorkspaceHistoricalFailure | null,
  referenceNowMs: number,
): WorkspaceHistoricalFailureView | null {
  if (failure === null) {
    return null;
  }
  return {
    failureId: failure.failure_id,
    stage: failure.stage,
    failedAtMs: failure.failed_at_ms,
    ageLabel: formatAge(failure.failed_at_ms, referenceNowMs),
    taskId: failure.task_id,
    attemptNo: failure.attempt_no,
    runId: failure.run_id,
    outboxId: failure.outbox_id,
    reason: failure.reason,
    supersededOrResolved: failure.superseded_or_resolved,
    statusLabel: failure.superseded_or_resolved
      ? 'Historical failure (superseded)'
      : 'Historical failure',
  };
}

function projectAttentionStateView(
  attention: WorkspaceAttentionState | null,
  referenceNowMs: number,
): WorkspaceAttentionView | null {
  if (attention === null || attention.kind === 'none') {
    return null;
  }
  const fallbackReason = `${formatAttentionKindLabel(attention.kind)} requires operator attention.`;
  return {
    kind: attention.kind,
    kindLabel: formatAttentionKindLabel(attention.kind),
    reason: attention.reason.length > 0 ? attention.reason : fallbackReason,
    nextStep: attention.next_step,
    blockingHoldIds: [...attention.blocking_hold_ids],
    blockingOutboxIds: [...attention.blocking_outbox_ids],
    questionId: attention.question_id,
    updatedAtMs: attention.updated_at_ms,
    ageLabel: formatAge(attention.updated_at_ms, referenceNowMs),
  };
}

function projectWorkspaceTaskAttemptView(
  attempt: WorkspaceTaskAttempt,
  currentAttemptNo: number,
  referenceNowMs: number,
): WorkspaceTaskAttemptView {
  const projected = projectTaskStatus(attempt.status);
  const concreteFailure =
    projected.tone === 'danger' || hasConcreteFailureToken(attempt.status);
  const statusLabel =
    projected.tone === 'unknown' && concreteFailure
      ? attempt.status.toLowerCase().includes('time')
        ? 'Timed out'
        : 'Failed'
      : projected.label;
  const tone: StatusDotTone = concreteFailure
    ? 'danger'
    : mapTaskToneToDotTone(projected.tone);
  const ts = attempt.finished_at_ms ?? attempt.started_at_ms;
  return {
    attemptNo: attempt.attempt_no,
    isCurrentAttempt: currentAttemptNo > 0 && attempt.attempt_no === currentAttemptNo,
    statusRaw: attempt.status,
    statusLabel,
    tone,
    runId: attempt.run_id,
    workerId: attempt.worker_id,
    outboxId: attempt.outbox_id,
    executor: attempt.executor,
    startedAtMs: attempt.started_at_ms,
    finishedAtMs: attempt.finished_at_ms,
    ageLabel: formatAge(ts, referenceNowMs),
    summary: attempt.summary,
  };
}

function projectWorkspaceTaskView(
  task: WorkspaceTaskHistory,
  referenceNowMs: number,
): WorkspaceTaskView {
  // Strictly enforce backend-verified task acceptance proof; never derive new proof from ID formats.
  const acceptanceProof = normalizeWorkspaceAcceptanceProofForTask(task.acceptance_proof);
  const isVerifiedAccepted = acceptanceProof.status === 'verified_accepted';

  let acceptanceLabel: string;
  switch (acceptanceProof.status) {
    case 'verified_accepted':
      acceptanceLabel = 'Verified accepted';
      break;
    case 'pending_review':
      acceptanceLabel = 'Pending review';
      break;
    case 'rejected':
      acceptanceLabel = 'Rejected';
      break;
    case 'not_accepted':
      acceptanceLabel = 'Not accepted';
      break;
    case 'unknown': {
      const lower = task.status.trim().toLowerCase();
      acceptanceLabel =
        lower === 'completed' || lower === 'accepted' || lower === 'done'
          ? 'Unverified completion (no backend acceptance proof)'
          : 'Acceptance not yet verified';
      break;
    }
  }

  const sortedAttempts = [...task.attempts]
    .sort((a, b) => a.attempt_no - b.attempt_no)
    .map((attempt) =>
      projectWorkspaceTaskAttemptView(attempt, task.current_attempt_no, referenceNowMs),
    );

  const currentAttempt =
    task.current_attempt_no > 0
      ? (sortedAttempts.find((a) => a.attemptNo === task.current_attempt_no) ?? null)
      : null;
  const pastAttempts = sortedAttempts.filter(
    (a) => task.current_attempt_no <= 0 || a.attemptNo !== task.current_attempt_no,
  );

  const currentAttemptNotice =
    task.current_attempt_no > 0 && currentAttempt === null
      ? `Current attempt #${task.current_attempt_no} is recorded on the task, while detailed attempt entry #${task.current_attempt_no} is not present in this bounded window.`
      : null;

  const candidates: WorkspaceTaskCandidateView[] = task.candidates.map((cand) => {
    const verificationLabel =
      cand.verification_passed === true
        ? 'Verification passed'
        : cand.verification_passed === false
          ? 'Verification failed'
          : 'Verification not reported';
    return {
      candidateId: cand.candidate_id,
      attemptNo: cand.attempt_no,
      workerId: cand.worker_id,
      headCommitSha: cand.head_commit_sha,
      treeSha: cand.tree_sha,
      verificationPassed: cand.verification_passed,
      verificationLabel,
      reviewStatusRaw: cand.review_status,
      reviewVerdictId: cand.review_verdict_id,
      reviewReceiptId: cand.review_receipt_id,
      producedAtMs: cand.produced_at_ms,
      ageLabel: formatAge(cand.produced_at_ms, referenceNowMs),
      summary: cand.summary,
      isActiveCandidate:
        task.active_candidate_id !== null && cand.candidate_id === task.active_candidate_id,
      isAcceptedByVerifiedProof:
        isVerifiedAccepted && acceptanceProof.accepted_candidate_id === cand.candidate_id,
    };
  });

  const historicalFailure = projectHistoricalFailureView(
    task.historical_failure,
    referenceNowMs,
  );
  const currentAttention = projectAttentionStateView(
    task.current_attention,
    referenceNowMs,
  );

  const projectedStatus = projectTaskStatus(task.status);
  const concreteFailure =
    currentAttention === null &&
    !isVerifiedAccepted &&
    (projectedStatus.tone === 'danger' || hasConcreteFailureToken(task.status));

  let statusLabel = projectedStatus.label;
  let tone: StatusDotTone = mapTaskToneToDotTone(projectedStatus.tone);

  if (currentAttention !== null) {
    statusLabel = currentAttention.kindLabel;
    tone = 'attention';
  } else if (isVerifiedAccepted) {
    statusLabel = 'Verified accepted';
    tone = 'success';
  } else if (
    projectedStatus.tone === 'ok' ||
    task.status.trim().toLowerCase() === 'completed' ||
    task.status.trim().toLowerCase() === 'accepted'
  ) {
    statusLabel = 'Completed (unverified acceptance)';
    tone = 'neutral';
  } else if (concreteFailure) {
    statusLabel = task.status.toLowerCase().includes('time') ? 'Timed out' : 'Failed';
    tone = 'danger';
  }

  return {
    taskId: task.task_id,
    title: task.title,
    role: task.role,
    difficulty: task.difficulty,
    stage: task.stage,
    statusRaw: task.status,
    statusLabel,
    tone,
    dependsOn: [...task.depends_on],
    currentAttemptNo: task.current_attempt_no,
    currentAttempt,
    pastAttempts,
    allAttempts: sortedAttempts,
    currentAttemptNotice,
    candidates,
    activeCandidateId: task.active_candidate_id,
    recordedAcceptedCandidateId: task.accepted_candidate_id,
    historicalFailure,
    currentAttention,
    acceptanceProof,
    isVerifiedAccepted,
    acceptanceLabel,
  };
}

function buildJobTruncationNotices(truncation: WorkspaceJobHistoryTruncation): string[] {
  const notices: string[] = [];
  if (truncation.tasks_omitted > 0) {
    notices.push(`Bounded task list: ${truncation.tasks_omitted} task(s) omitted.`);
  }
  if (truncation.attempts_omitted > 0) {
    notices.push(
      `Bounded attempt history: ${truncation.attempts_omitted} older attempt(s) omitted.`,
    );
  }
  if (truncation.candidates_omitted > 0) {
    notices.push(
      `Bounded candidate list: ${truncation.candidates_omitted} older candidate(s) omitted.`,
    );
  }
  if (truncation.runs_omitted > 0) {
    notices.push(`Bounded run ledger: ${truncation.runs_omitted} older run(s) omitted.`);
  }
  if (truncation.outbox_omitted > 0) {
    notices.push(
      `Bounded outbox ledger: ${truncation.outbox_omitted} older outbox record(s) omitted.`,
    );
  }
  if (truncation.combined_candidates_omitted > 0) {
    notices.push(
      `Bounded combined candidates: ${truncation.combined_candidates_omitted} older combined candidate(s) omitted.`,
    );
  }
  return notices;
}

function projectWorkspaceJobView(
  history: WorkspaceJobHistory,
  identity: WorkspaceChatIdentity,
  referenceNowMs: number,
): WorkspaceJobView {
  // Strictly enforce backend-verified job acceptance proof; never derive new proof from ID formats.
  const acceptanceProof = normalizeWorkspaceAcceptanceProofForJob(history.acceptance_proof);
  const isVerifiedAccepted = acceptanceProof.status === 'verified_accepted';

  let acceptanceLabel: string;
  switch (acceptanceProof.status) {
    case 'verified_accepted':
      acceptanceLabel = 'Verified accepted';
      break;
    case 'pending_review':
      acceptanceLabel = 'Pending closure review';
      break;
    case 'rejected':
      acceptanceLabel = 'Closure rejected';
      break;
    case 'not_accepted':
      acceptanceLabel = 'Not accepted';
      break;
    case 'unknown': {
      const execLower = history.job.execution_status.trim().toLowerCase();
      acceptanceLabel =
        history.job.state === 'completed' ||
        execLower === 'accepted' ||
        execLower === 'completed'
          ? 'Awaiting backend-verified acceptance proof'
          : 'Acceptance not yet verified';
      break;
    }
  }

  const tasks = history.tasks.map((t) => projectWorkspaceTaskView(t, referenceNowMs));
  const combinedCandidates: WorkspaceCombinedCandidateView[] =
    history.combined_candidates.map((cc) => ({
      combinedCandidateId: cc.combined_candidate_id,
      jobRevision: cc.job_revision,
      statusRaw: cc.status,
      finalCommitSha: cc.final_commit_sha,
      finalTreeSha: cc.final_tree_sha,
      candidateDigest: cc.candidate_digest,
      closureReservationId: cc.closure_reservation_id,
      checkCount: cc.check_count,
      producedAtMs: cc.produced_at_ms,
      ageLabel: formatAge(cc.produced_at_ms, referenceNowMs),
      isAcceptedByVerifiedProof:
        isVerifiedAccepted &&
        acceptanceProof.combined_candidate_id === cc.combined_candidate_id,
    }));

  const resultView: WorkspaceJobResultView | null =
    history.result !== null
      ? (() => {
          const stageDesc = describeDeliveryStage(history.result.delivery_stage);
          return {
            outcome: history.result.outcome,
            summary: history.result.summary,
            producedAtMs: history.result.produced_at_ms,
            ageLabel: formatAge(history.result.produced_at_ms, referenceNowMs),
            deliveryStage: history.result.delivery_stage,
            deliveryStageLabel: stageDesc.label,
            deliveryStageNote: stageDesc.note,
            closureVerdictId: history.result.closure_verdict_id,
          };
        })()
      : null;

  const historicalFailure = projectHistoricalFailureView(
    history.historical_failure,
    referenceNowMs,
  );
  const currentAttention = projectAttentionStateView(
    history.current_attention,
    referenceNowMs,
  );
  const taskAttention =
    currentAttention ?? tasks.find((t) => t.currentAttention !== null)?.currentAttention ?? null;

  let statusLabel: string;
  let tone: StatusDotTone;
  if (taskAttention !== null) {
    statusLabel = taskAttention.kindLabel;
    tone = 'attention';
  } else if (history.job.state === 'needs_user') {
    statusLabel = 'Needs user input';
    tone = 'attention';
  } else if (isVerifiedAccepted) {
    statusLabel = 'Verified accepted';
    tone = 'success';
  } else if (history.job.state === 'completed') {
    statusLabel = 'Completed (unverified acceptance)';
    tone = 'neutral';
  } else if (history.job.state === 'reviewing') {
    statusLabel = 'Reviewing';
    tone = 'working';
  } else if (history.job.state === 'running') {
    statusLabel = 'Running';
    tone = 'working';
  } else if (history.job.state === 'planning') {
    statusLabel = 'Planning';
    tone = 'working';
  } else if (history.job.state === 'ready') {
    statusLabel = 'Ready';
    tone = 'neutral';
  } else if (history.job.state === 'failed') {
    statusLabel = 'Failed';
    tone = 'danger';
  } else {
    statusLabel = 'Status unknown';
    tone = 'neutral';
  }

  const truncationNotices = buildJobTruncationNotices(history.truncation);

  return {
    jobKey: `${history.job.source_id}\u0000${history.job.job_id}`,
    sourceId: history.job.source_id,
    jobId: history.job.job_id,
    bindingId: history.job.binding_id,
    chatIdentity: identity,
    title: history.job.title,
    summary: history.job.summary,
    jobRevision: history.job.job_revision,
    stateRaw: history.job.state,
    phase: history.job.phase,
    approvalStatus: history.job.approval_status,
    executionStatus: history.job.execution_status,
    reviewState: history.job.review_state,
    statusLabel,
    tone,
    createdAtMs: history.job.created_at_ms,
    updatedAtMs: history.job.updated_at_ms,
    ageLabel: formatAge(history.job.updated_at_ms, referenceNowMs),
    tasks,
    runs: history.runs.map((r) => ({ ...r })),
    outbox: history.outbox.map((o) => ({ ...o })),
    combinedCandidates,
    result: resultView,
    links: {
      ...history.links,
      follow_up_job_ids: [...history.links.follow_up_job_ids],
      architect_advice_ids: [...history.links.architect_advice_ids],
      source_request_ids: [...history.links.source_request_ids],
    },
    historicalFailure,
    currentAttention,
    acceptanceProof,
    isVerifiedAccepted,
    acceptanceLabel,
    truncation: { ...history.truncation },
    isTruncated: truncationNotices.length > 0,
    truncationNotices,
  };
}

function formatExecutorCompactLabel(
  executor: ObservationExecutor | null | undefined,
  isLocalMcp: boolean,
): { label: string; modelLabel: string | null } {
  if (isLocalMcp) {
    return { label: 'Threadlane MCP · local', modelLabel: null };
  }
  if (!hasExecutorRecord(executor)) {
    return { label: 'Executor not recorded', modelLabel: null };
  }
  const roleRaw = executor?.role?.trim() ?? '';
  const roleLabel =
    roleRaw.toLowerCase() === 'coder'
      ? 'Worker'
      : roleRaw.length > 0
        ? roleRaw
        : null;
  const toolLabel = executor?.tool?.trim() || executor?.provider?.trim() || null;
  const label =
    [roleLabel, toolLabel].filter((v): v is string => Boolean(v)).join(' · ') ||
    'Executor recorded';
  const modelToken =
    executor?.observed_model?.trim() || executor?.requested_model?.trim() || null;
  const effortToken =
    (executor?.observed_model?.trim()
      ? executor?.observed_effort?.trim()
      : executor?.requested_effort?.trim()) || null;
  const modelLabel =
    modelToken !== null
      ? effortToken !== null
        ? `${formatFriendlyModel(modelToken)} · ${effortToken}`
        : formatFriendlyModel(modelToken)
      : null;
  return { label, modelLabel };
}

function projectWorkspaceActivityItemView(
  req: ObservationRequest,
  chatIdentity: WorkspaceChatIdentity,
  referenceNowMs: number,
): WorkspaceActivityItemView {
  const isLocalMcpCall = isLocalMcpRequestRecord(req);
  const baseState = formatObservationLifecycleLabel(req.state);

  let stateLabel = baseState.label;
  let tone = baseState.tone;
  let statusNote: string | null = null;

  if (isLocalMcpCall && req.state === 'completed') {
    stateLabel = 'MCP call finished';
    tone = 'neutral';
    statusNote = 'Local MCP call finished; MCP calls never complete a chat or job.';
  } else if (!isLocalMcpCall && req.state === 'completed') {
    stateLabel = 'Call finished';
    tone = 'neutral';
    statusNote =
      'Execution call finished; authoritative job completion requires backend-verified acceptance.';
  }

  const children: WorkspaceActivityChildCallView[] = req.children.map((call) => {
    const childLocalMcp = isLocalMcpCallExecutorOrOp(call.executor, null);
    const childBase = formatObservationLifecycleLabel(call.state);
    const childExec = formatExecutorCompactLabel(call.executor, childLocalMcp);
    const childDur = projectDuration(
      {
        state: call.state,
        started_at_ms: call.started_at_ms,
        finished_at_ms: call.finished_at_ms,
        duration_ms: call.duration_ms,
      },
      referenceNowMs,
    );
    const childUsage = projectUsage(call.usage, call.executor, false);
    return {
      call,
      callId: call.id,
      title: sanitizeWorkspaceLabel(call.title, 'Call'),
      stateRaw: call.state,
      stateLabel:
        childLocalMcp && call.state === 'completed'
          ? 'MCP call finished'
          : childBase.label,
      tone:
        childLocalMcp && call.state === 'completed' ? 'neutral' : childBase.tone,
      running: call.state === 'running',
      isLocalMcpCall: childLocalMcp,
      executorLabel: childExec.label,
      brand: resolveExecutorBrand(call.executor),
      durationLabel: childDur.text,
      usageLabel: childUsage.summary,
      summary: call.summary !== null ? sanitizeWorkspaceSummary(call.summary) : null,
    };
  });

  const reportedChildCount = Math.max(
    children.length,
    Number.isFinite(req.children_count) ? Math.max(0, Math.trunc(req.children_count)) : 0,
  );
  const omittedChildCallCount = Math.max(0, reportedChildCount - children.length);
  const truncationNotice =
    omittedChildCallCount > 0
      ? `Bounded child calls: showing ${children.length} of ${reportedChildCount} child call(s) (${omittedChildCallCount} omitted).`
      : null;

  const deliveries: WorkspaceActivityDeliveryView[] = req.delivery.map((del) => {
    const stageInfo = describeDeliveryStage(del.stage);
    return {
      record: del,
      stage: del.stage,
      stageLabel: stageInfo.label,
      stageNote: stageInfo.note,
      atMs: del.at_ms,
      ageLabel: formatAge(del.at_ms, referenceNowMs),
      recipient: del.recipient,
      summary: sanitizeWorkspaceSummary(del.summary),
    };
  });

  const execDisplay = formatExecutorCompactLabel(req.executor, isLocalMcpCall);
  const duration = projectDuration(req, referenceNowMs);
  const usage = projectUsage(req.usage, req.executor, children.length > 0);
  const running = req.state === 'running' || children.some((c) => c.running);
  if (running) {
    tone = 'working';
  }

  const historicalActorSelectedModel =
    typeof req.actor.selected_model === 'string' && req.actor.selected_model.trim().length > 0
      ? req.actor.selected_model.trim()
      : null;

  return {
    requestKey: `${req.source_id}\u0000${req.id}`,
    sourceId: req.source_id,
    requestId: req.id,
    jobId: validateCorrelationId(req.job_id),
    parentRequestId: validateCorrelationId(req.parent_request_id),
    chatIdentity,
    title: sanitizeWorkspaceLabel(req.title, formatRecordedOperation(req.operation)),
    operationRaw: req.operation,
    operationLabel: formatRecordedOperation(req.operation),
    stateRaw: req.state,
    stateLabel,
    tone,
    running,
    isLocalMcpCall,
    completesChat: false,
    statusNote,
    actor: req.actor,
    historicalActorSelectedModel,
    executor: req.executor,
    executorLabel: execDisplay.label,
    executorModelLabel: execDisplay.modelLabel,
    brand: resolveExecutorBrand(req.executor),
    startedAtMs: req.started_at_ms,
    finishedAtMs: req.finished_at_ms,
    updatedAtMs: req.updated_at_ms,
    ageLabel: formatAge(req.updated_at_ms, referenceNowMs),
    durationLabel: duration.text,
    usageLabel: usage.summary,
    summary: req.summary !== null ? sanitizeWorkspaceSummary(req.summary) : null,
    deliveries,
    children,
    reportedChildCount,
    omittedChildCallCount,
    truncationNotice,
  };
}

function projectMessageItemView(
  item: WorkspaceMessageItem,
  referenceNowMs: number,
): WorkspaceMessageItemView {
  const completenessNotices: string[] = [];
  if (item.completeness.summary_truncated) {
    if (item.completeness.original_char_count !== null) {
      completenessNotices.push(
        `Summary truncated to ${item.completeness.max_summary_chars} characters (from ${item.completeness.original_char_count} characters).`,
      );
    } else {
      completenessNotices.push(
        `Summary truncated to ${item.completeness.max_summary_chars} characters.`,
      );
    }
  }
  if (item.completeness.redacted) {
    completenessNotices.push('Sensitive metadata was redacted from this summary.');
  }
  if (item.completeness.missing_fields.length > 0) {
    completenessNotices.push(
      `Incomplete metadata: missing ${item.completeness.missing_fields.join(', ')}.`,
    );
  }
  if (item.completeness.incomplete_reason !== null) {
    completenessNotices.push(
      `Incomplete record note: ${item.completeness.incomplete_reason}`,
    );
  }

  return {
    itemId: item.item_id,
    sourceKind: item.source_kind,
    sourceKindLabel: formatSafeMessageSourceKindLabel(item.source_kind),
    sourceRecordId: item.source_record_id,
    recordedAtMs: item.source_order.recorded_at_ms,
    ageLabel: formatAge(item.source_order.recorded_at_ms, referenceNowMs),
    sourceSeq: item.source_order.source_seq,
    subIndex: item.source_order.sub_index,
    actorRole: item.actor_role,
    jobId: item.scope.job_id,
    taskId: item.scope.task_id,
    runId: item.scope.run_id,
    attemptNo: item.scope.attempt_no,
    summary: item.summary,
    completeness: {
      ...item.completeness,
      missing_fields: [...item.completeness.missing_fields],
    },
    isIncomplete: completenessNotices.length > 0,
    completenessNotices,
  };
}

function computeChatWorkerSummary(
  jobHistories: readonly WorkspaceJobHistory[],
  obsJobsById: ReadonlyMap<string, ObservationJob>,
  activityItems: readonly WorkspaceActivityItemView[],
): WorkspaceWorkerSummaryView {
  const activeNonMcpRequestsByJob = new Map<string, number>();
  let activeUnlinkedWorkerRequests = 0;
  let completedUnlinkedWorkerRequests = 0;
  let failedUnlinkedWorkerRequests = 0;

  for (const item of activityItems) {
    if (item.isLocalMcpCall) {
      continue;
    }
    if (item.jobId !== null) {
      if (item.running) {
        activeNonMcpRequestsByJob.set(
          item.jobId,
          (activeNonMcpRequestsByJob.get(item.jobId) ?? 0) + 1,
        );
      }
    } else if (item.running) {
      activeUnlinkedWorkerRequests += 1;
    } else if (item.stateRaw === 'completed') {
      completedUnlinkedWorkerRequests += 1;
    } else if (item.stateRaw === 'failed') {
      failedUnlinkedWorkerRequests += 1;
    }
  }

  let activeWorkerCount = activeUnlinkedWorkerRequests;
  let completedWorkerRunCount = completedUnlinkedWorkerRequests;
  let failedWorkerRunCount = failedUnlinkedWorkerRequests;
  const seenJobIds = new Set<string>();

  for (const history of jobHistories) {
    const jobId = history.job.job_id;
    seenJobIds.add(jobId);

    const nonMcpRuns = history.runs.filter(
      (run) => !isLocalMcpCallExecutorOrOp(run.executor, null),
    );
    const activeRunsCount = nonMcpRuns.filter((run) =>
      isActiveWorkerRunStatus(run.status),
    ).length;
    const completedRunsCount = nonMcpRuns.filter((run) =>
      isCompletedWorkerRunStatus(run.status),
    ).length;
    const failedRunsCount = nonMcpRuns.filter((run) =>
      hasConcreteFailureToken(run.status),
    ).length;

    let activeAttemptsCount = 0;
    let completedAttemptsCount = 0;
    let failedAttemptsCount = 0;
    if (nonMcpRuns.length === 0) {
      for (const task of history.tasks) {
        for (const attempt of task.attempts) {
          if (isLocalMcpCallExecutorOrOp(attempt.executor, null)) {
            continue;
          }
          if (isActiveWorkerRunStatus(attempt.status)) {
            activeAttemptsCount += 1;
          } else if (isCompletedWorkerRunStatus(attempt.status)) {
            completedAttemptsCount += 1;
          } else if (hasConcreteFailureToken(attempt.status)) {
            failedAttemptsCount += 1;
          }
        }
      }
    }

    const reqRunningForJob = activeNonMcpRequestsByJob.get(jobId) ?? 0;
    const obsJob = obsJobsById.get(`${history.job.source_id}\u0000${jobId}`);
    const legacyCallFallback =
      nonMcpRuns.length === 0 &&
      activeAttemptsCount === 0 &&
      reqRunningForJob === 0 &&
      obsJob !== undefined &&
      obsJob.active_call_count > 0
        ? obsJob.active_call_count
        : 0;

    activeWorkerCount += Math.max(
      activeRunsCount,
      activeAttemptsCount,
      reqRunningForJob,
      legacyCallFallback,
    );
    completedWorkerRunCount +=
      nonMcpRuns.length > 0 ? completedRunsCount : completedAttemptsCount;
    failedWorkerRunCount +=
      nonMcpRuns.length > 0 ? failedRunsCount : failedAttemptsCount;
  }

  for (const [jobId, count] of activeNonMcpRequestsByJob.entries()) {
    if (!seenJobIds.has(jobId)) {
      activeWorkerCount += count;
    }
  }

  const hasActiveWorkers = activeWorkerCount > 0;
  const badgeLabel = hasActiveWorkers
    ? activeWorkerCount === 1
      ? '1 worker running'
      : `${activeWorkerCount} workers running`
    : null;
  const statusLabel = badgeLabel ?? 'No active workers';

  return {
    activeWorkerCount,
    completedWorkerRunCount,
    failedWorkerRunCount,
    hasActiveWorkers,
    badgeLabel,
    statusLabel,
  };
}

function computeChatJobSummary(
  jobViews: readonly WorkspaceJobView[],
  summaryActiveJobCount: number,
): WorkspaceChatJobSummaryView {
  const totalJobCount = jobViews.length;
  const computedActiveJobs = jobViews.filter((j) =>
    isActiveObservationJobState(j.stateRaw),
  ).length;
  const activeJobCount = Math.max(computedActiveJobs, summaryActiveJobCount);
  const verifiedAcceptedJobCount = jobViews.filter((j) => j.isVerifiedAccepted).length;
  const pendingAcceptanceJobCount = jobViews.filter(
    (j) =>
      !j.isVerifiedAccepted &&
      (j.stateRaw === 'completed' ||
        j.executionStatus.trim().toLowerCase() === 'accepted' ||
        j.executionStatus.trim().toLowerCase() === 'completed'),
  ).length;
  const failedJobCount = jobViews.filter((j) => j.stateRaw === 'failed').length;
  const needsAttentionJobCount = jobViews.filter(
    (j) =>
      j.currentAttention !== null ||
      j.stateRaw === 'needs_user' ||
      j.tasks.some((t) => t.currentAttention !== null),
  ).length;

  let statusLabel: string;
  if (totalJobCount === 0 && activeJobCount === 0) {
    statusLabel = 'No jobs';
  } else if (needsAttentionJobCount > 0) {
    statusLabel =
      needsAttentionJobCount === 1
        ? '1 job needs attention'
        : `${needsAttentionJobCount} jobs need attention`;
  } else if (activeJobCount > 0) {
    statusLabel =
      activeJobCount === 1 ? '1 active job' : `${activeJobCount} active jobs`;
  } else if (pendingAcceptanceJobCount > 0) {
    statusLabel = 'Awaiting verified acceptance';
  } else if (failedJobCount > 0 && verifiedAcceptedJobCount === 0) {
    statusLabel = failedJobCount === 1 ? '1 failed job' : `${failedJobCount} failed jobs`;
  } else if (verifiedAcceptedJobCount === totalJobCount && totalJobCount > 0) {
    statusLabel =
      verifiedAcceptedJobCount === 1
        ? '1 verified accepted job'
        : `${verifiedAcceptedJobCount} verified accepted jobs`;
  } else {
    statusLabel = `${totalJobCount} recorded job(s)`;
  }

  return {
    totalJobCount,
    activeJobCount,
    verifiedAcceptedJobCount,
    pendingAcceptanceJobCount,
    failedJobCount,
    needsAttentionJobCount,
    statusLabel,
  };
}

function collectChatBlockers(
  chatKey: string,
  nativeState: WorkspaceNativeChatStateView,
  jobViews: readonly WorkspaceJobView[],
  activityItems: readonly WorkspaceActivityItemView[],
  referenceNowMs: number,
): WorkspaceChatBlockerView[] {
  const blockers: WorkspaceChatBlockerView[] = [];
  const jobsWithExplicitAttention = new Set<string>();

  if (
    nativeState.availability === 'available' &&
    (nativeState.activity === 'waiting_for_approval' ||
      nativeState.activity === 'waiting_for_input')
  ) {
    const kind: WorkspaceAttentionKind =
      nativeState.activity === 'waiting_for_approval'
        ? 'needs_approval'
        : 'needs_user_input';
    blockers.push({
      key: `${chatKey}\u0000blocker\u0000native`,
      origin: 'native_chat',
      kind,
      kindLabel: formatAttentionKindLabel(kind),
      reason:
        nativeState.activity === 'waiting_for_approval'
          ? 'Coworker is waiting for human approval in the native chat.'
          : 'Coworker is waiting for user input in the native chat.',
      nextStep: 'Open the chat in Codex to respond.',
      jobId: null,
      taskId: null,
      requestId: null,
      questionId: null,
      blockingHoldIds: [],
      blockingOutboxIds: [],
      updatedAtMs: nativeState.observedAtMs,
      ageLabel: formatAge(nativeState.observedAtMs, referenceNowMs),
    });
  }

  for (const job of jobViews) {
    if (job.currentAttention !== null) {
      jobsWithExplicitAttention.add(job.jobId);
      blockers.push({
        key: `${chatKey}\u0000blocker\u0000job\u0000${job.jobId}`,
        origin: 'job_attention',
        kind: job.currentAttention.kind,
        kindLabel: job.currentAttention.kindLabel,
        reason: job.currentAttention.reason,
        nextStep: job.currentAttention.nextStep,
        jobId: job.jobId,
        taskId: null,
        requestId: null,
        questionId: job.currentAttention.questionId,
        blockingHoldIds: [...job.currentAttention.blockingHoldIds],
        blockingOutboxIds: [...job.currentAttention.blockingOutboxIds],
        updatedAtMs: job.currentAttention.updatedAtMs ?? job.updatedAtMs,
        ageLabel: formatAge(
          job.currentAttention.updatedAtMs ?? job.updatedAtMs,
          referenceNowMs,
        ),
      });
    }

    for (const task of job.tasks) {
      if (task.currentAttention !== null) {
        jobsWithExplicitAttention.add(job.jobId);
        blockers.push({
          key: `${chatKey}\u0000blocker\u0000task\u0000${job.jobId}\u0000${task.taskId}`,
          origin: 'task_attention',
          kind: task.currentAttention.kind,
          kindLabel: task.currentAttention.kindLabel,
          reason: task.currentAttention.reason,
          nextStep: task.currentAttention.nextStep,
          jobId: job.jobId,
          taskId: task.taskId,
          requestId: null,
          questionId: task.currentAttention.questionId,
          blockingHoldIds: [...task.currentAttention.blockingHoldIds],
          blockingOutboxIds: [...task.currentAttention.blockingOutboxIds],
          updatedAtMs: task.currentAttention.updatedAtMs ?? job.updatedAtMs,
          ageLabel: formatAge(
            task.currentAttention.updatedAtMs ?? job.updatedAtMs,
            referenceNowMs,
          ),
        });
      }
    }

    if (job.stateRaw === 'needs_user' && !jobsWithExplicitAttention.has(job.jobId)) {
      jobsWithExplicitAttention.add(job.jobId);
      blockers.push({
        key: `${chatKey}\u0000blocker\u0000needs_user\u0000${job.jobId}`,
        origin: 'job_needs_user',
        kind: 'needs_user_input',
        kindLabel: 'Needs user input',
        reason: job.summary ?? `Job "${job.title}" is waiting for user input.`,
        nextStep: 'Review the blocked job and respond in Codex.',
        jobId: job.jobId,
        taskId: null,
        requestId: null,
        questionId: null,
        blockingHoldIds: [],
        blockingOutboxIds: [],
        updatedAtMs: job.updatedAtMs,
        ageLabel: formatAge(job.updatedAtMs, referenceNowMs),
      });
    }
  }

  for (const item of activityItems) {
    if (item.stateRaw !== 'waiting_for_user') {
      continue;
    }
    if (item.jobId !== null && jobsWithExplicitAttention.has(item.jobId)) {
      continue;
    }
    blockers.push({
      key: `${chatKey}\u0000blocker\u0000req\u0000${item.requestId}`,
      origin: 'request_waiting_for_user',
      kind: 'needs_user_input',
      kindLabel: 'Waiting for user',
      reason: item.summary ?? `Request "${item.title}" is waiting for user input.`,
      nextStep: 'Respond in Codex to continue.',
      jobId: item.jobId,
      taskId: null,
      requestId: item.requestId,
      questionId: null,
      blockingHoldIds: [],
      blockingOutboxIds: [],
      updatedAtMs: item.updatedAtMs,
      ageLabel: item.ageLabel,
    });
  }

  return blockers;
}

interface DiscoveredChatAccumulator {
  chatKey: string;
  identity: WorkspaceChatIdentity;
  chatSummary: WorkspaceChatSummary | null;
  nativeSample: WorkspaceNativeSample | null;
  jobHistories: WorkspaceJobHistory[];
  messageItems: WorkspaceMessageItem[];
  activityRequests: ObservationRequest[];
  obsJobs: ObservationJob[];
}

function compareWorkspaceChatSidebarItems(
  left: WorkspaceChatSidebarItem,
  right: WorkspaceChatSidebarItem,
): number {
  if (left.hasCurrentBlocker !== right.hasCurrentBlocker) {
    return left.hasCurrentBlocker ? -1 : 1;
  }
  const leftWorking =
    (left.nativeState.availability === 'available' &&
      left.nativeState.activity === 'working') ||
    left.workerSummary.hasActiveWorkers ||
    left.jobSummary.activeJobCount > 0;
  const rightWorking =
    (right.nativeState.availability === 'available' &&
      right.nativeState.activity === 'working') ||
    right.workerSummary.hasActiveWorkers ||
    right.jobSummary.activeJobCount > 0;
  if (leftWorking !== rightWorking) {
    return leftWorking ? -1 : 1;
  }
  const leftTs = left.updatedAtMs ?? Number.MIN_SAFE_INTEGER;
  const rightTs = right.updatedAtMs ?? Number.MIN_SAFE_INTEGER;
  if (rightTs !== leftTs) {
    return rightTs - leftTs;
  }
  return compareUtf8Lexical(left.chatKey, right.chatKey);
}

function compareWorkspaceProjectGroups(
  left: WorkspaceProjectGroupView,
  right: WorkspaceProjectGroupView,
): number {
  if (left.hasNeedsHuman !== right.hasNeedsHuman) {
    return left.hasNeedsHuman ? -1 : 1;
  }
  if (left.hasActiveWork !== right.hasActiveWork) {
    return left.hasActiveWork ? -1 : 1;
  }
  const leftTs = left.latestUpdatedAtMs ?? Number.MIN_SAFE_INTEGER;
  const rightTs = right.latestUpdatedAtMs ?? Number.MIN_SAFE_INTEGER;
  if (rightTs !== leftTs) {
    return rightTs - leftTs;
  }
  return compareUtf8Lexical(left.projectKey, right.projectKey);
}

function buildWorkspaceProjectGroups(
  chats: readonly WorkspaceChatSidebarItem[],
  lifecycle: WorkspaceChatLifecycle,
  referenceNowMs: number,
): WorkspaceProjectGroupView[] {
  const byProjectKey = new Map<string, WorkspaceChatSidebarItem[]>();
  for (const chat of chats) {
    let bucket = byProjectKey.get(chat.projectKey);
    if (!bucket) {
      bucket = [];
      byProjectKey.set(chat.projectKey, bucket);
    }
    bucket.push(chat);
  }

  const groups: WorkspaceProjectGroupView[] = [];
  for (const [projectKey, groupChats] of byProjectKey.entries()) {
    const sortedChats = [...groupChats].sort(compareWorkspaceChatSidebarItems);
    const first = sortedChats[0]!;

    let latestUpdatedAtMs: number | null = null;
    let activeWorkerCount = 0;
    let workingCoworkerCount = 0;
    let needsHumanCount = 0;
    let hasActiveJob = false;

    for (const chat of sortedChats) {
      activeWorkerCount += chat.workerSummary.activeWorkerCount;
      if (
        chat.nativeState.availability === 'available' &&
        chat.nativeState.activity === 'working'
      ) {
        workingCoworkerCount += 1;
      }
      if (chat.jobSummary.activeJobCount > 0) {
        hasActiveJob = true;
      }
      needsHumanCount += chat.needsHumanCount;
      if (
        chat.updatedAtMs !== null &&
        (latestUpdatedAtMs === null || chat.updatedAtMs > latestUpdatedAtMs)
      ) {
        latestUpdatedAtMs = chat.updatedAtMs;
      }
    }

    groups.push({
      projectKey,
      sourceId: first.identity.source_id,
      canonicalProjectScope: first.canonicalProjectScope,
      label: first.projectLabel,
      lifecycle,
      latestUpdatedAtMs,
      ageLabel: formatAge(latestUpdatedAtMs, referenceNowMs),
      chatCount: sortedChats.length,
      activeWorkerCount,
      workingCoworkerCount,
      needsHumanCount,
      hasActiveWork: activeWorkerCount > 0 || workingCoworkerCount > 0 || hasActiveJob,
      hasNeedsHuman: needsHumanCount > 0,
      chats: sortedChats,
    });
  }

  groups.sort(compareWorkspaceProjectGroups);
  return groups;
}

function buildSnapshotBoundsNotices(ws: WorkspaceSnapshot): string[] {
  const notices: string[] = [];
  if (ws.bounds.native_samples_omitted > 0) {
    notices.push(
      `Showing a bounded native sample window; ${ws.bounds.native_samples_omitted} native sample(s) were omitted.`,
    );
  }
  if (ws.bounds.job_histories_omitted > 0) {
    notices.push(
      `Showing a bounded job history window; ${ws.bounds.job_histories_omitted} job history record(s) were omitted.`,
    );
  }
  if (ws.observation.jobs_has_more || ws.bounds.jobs_capped) {
    notices.push(
      'Showing a bounded jobs window; additional jobs exist beyond this snapshot (jobs_has_more).',
    );
  }
  const messagesOmitted = Math.max(ws.messages.omitted_count, ws.bounds.messages_omitted);
  if (messagesOmitted > 0) {
    notices.push(
      `Showing a bounded messages window (max ${ws.messages.max_items}); ${messagesOmitted} message summary item(s) were omitted.`,
    );
  } else if (ws.messages.truncated) {
    notices.push('Message summaries window was truncated to fit safety bounds.');
  }
  if (ws.messages.has_more) {
    notices.push('Additional message summaries exist beyond the current bounded window.');
  }
  if (ws.observation.has_more || ws.bounds.requests_capped) {
    notices.push(
      `Showing a bounded activity request page${ws.bounds.requests_capped ? ' (capped at page limit)' : ''}; older activity requests exist beyond this window.`,
    );
  }
  return notices;
}

/**
 * Pure production workspace projection consuming `WorkspaceSnapshot` and shared
 * `workspace-contract.ts` helpers without reimplementing schema, sanitization,
 * or archive storage rules.
 */
export function projectWorkspaceSnapshot(
  snapshot: WorkspaceSnapshot | ObservationSnapshot | null,
  options: ProjectWorkspaceSnapshotOptions = {},
): WorkspaceProductionProjection {
  const lifecycleFilter: WorkspaceChatLifecycle =
    options.lifecycleFilter === 'archived'
      ? 'archived'
      : DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER;
  const requestedTab = normalizeWorkspaceChatTab(options.selectedTab);
  const loading = Boolean(options.loading);
  const error = options.error?.trim() ? options.error.trim() : null;

  // Resolve canonical per-chat archive preferences strictly from `WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY`.
  // Never read or convert legacy bulk `PROJECTS_ARCHIVE_STORAGE_KEY` cutoffs.
  const archivePreferences: WorkspaceChatArchivePreferenceMap =
    options.archivePreferences ??
    (options.preferenceStorage
      ? readWorkspaceChatArchivePreferences(options.preferenceStorage)
      : {});

  const emptyBounds: WorkspaceSnapshotBounds = {
    native_samples_omitted: 0,
    job_histories_omitted: 0,
    messages_omitted: 0,
    requests_capped: false,
    jobs_capped: false,
  };

  if (snapshot === null) {
    const fallbackNowMs =
      typeof options.nowMs === 'number' && Number.isFinite(options.nowMs)
        ? Math.trunc(options.nowMs)
        : Date.now();
    if (loading) {
      return {
        status: 'loading',
        unavailableReason: null,
        emptyMessage: null,
        isStale: false,
        staleReason: null,
        referenceNowMs: fallbackNowMs,
        lifecycleFilter,
        surfaceSeparation: WORKSPACE_SURFACE_SEPARATION,
        availableTabs: WORKSPACE_CHAT_SURFACE_TABS,
        defaultTab: DEFAULT_WORKSPACE_CHAT_TAB,
        activeProjects: [],
        archivedProjects: [],
        visibleProjects: [],
        activeChatCount: 0,
        archivedChatCount: 0,
        totalWorkingCoworkerCount: 0,
        totalActiveWorkerCount: 0,
        totalNeedsHumanCount: 0,
        activeNeedsHumanCount: 0,
        archivedNeedsHumanCount: 0,
        archivedBlockerCount: 0,
        archivedBlockedChats: [],
        archivedBlockerNotice: null,
        unavailableSourceCount: 0,
        unavailableSources: [],
        bounds: emptyBounds,
        boundsNotices: [],
        statusNotices: [],
        selectedChatKey: options.selectedChatKey ?? null,
        selectedChat: null,
        warnings: [],
        diagnostics: [],
      };
    }

    const unavailableReason =
      error ?? 'Workspace observation runtime is not connected or returned no snapshot.';
    return {
      status: 'unavailable',
      unavailableReason,
      emptyMessage: null,
      isStale: false,
      staleReason: null,
      referenceNowMs: fallbackNowMs,
      lifecycleFilter,
      surfaceSeparation: WORKSPACE_SURFACE_SEPARATION,
      availableTabs: WORKSPACE_CHAT_SURFACE_TABS,
      defaultTab: DEFAULT_WORKSPACE_CHAT_TAB,
      activeProjects: [],
      archivedProjects: [],
      visibleProjects: [],
      activeChatCount: 0,
      archivedChatCount: 0,
      totalWorkingCoworkerCount: 0,
      totalActiveWorkerCount: 0,
      totalNeedsHumanCount: 0,
      activeNeedsHumanCount: 0,
      archivedNeedsHumanCount: 0,
      archivedBlockerCount: 0,
      archivedBlockedChats: [],
      archivedBlockerNotice: null,
      unavailableSourceCount: 0,
      unavailableSources: [],
      bounds: emptyBounds,
      boundsNotices: [],
      statusNotices: [unavailableReason],
      selectedChatKey: options.selectedChatKey ?? null,
      selectedChat: null,
      warnings: [],
      diagnostics: [],
    };
  }

  const ws = coerceToNormalizedWorkspaceSnapshot(snapshot);
  const referenceNowMs =
    typeof options.nowMs === 'number' && Number.isFinite(options.nowMs)
      ? Math.trunc(options.nowMs)
      : ws.generated_at_ms > 0
        ? ws.generated_at_ms
        : ws.observation.generated_at_ms > 0
          ? ws.observation.generated_at_ms
          : Date.now();

  const sourcesById = new Map<string, ObservationSource>();
  const unavailableSources: WorkspaceUnavailableSourceView[] = [];
  for (const src of ws.observation.sources) {
    if (!sourcesById.has(src.id)) {
      sourcesById.set(src.id, src);
    }
    if (src.availability !== 'available') {
      const sourceLabel = sanitizeWorkspaceLabel(src.label, src.id || 'Unnamed source');
      const cleanedMsg =
        typeof src.message === 'string' && src.message.trim().length > 0
          ? sanitizeWorkspaceSummary(src.message)
          : null;
      const statusText =
        cleanedMsg !== null
          ? `Observation source "${sourceLabel}" (${src.id}) is unavailable: ${cleanedMsg}`
          : `Observation source "${sourceLabel}" (${src.id}) is unavailable.`;
      unavailableSources.push({
        source: src,
        sourceId: src.id,
        sourceLabel,
        projectPath: src.project_path,
        rawMessage: cleanedMsg,
        statusText,
      });
    }
  }

  const obsJobsByCompositeId = new Map<string, ObservationJob>();
  for (const obsJob of ws.observation.jobs) {
    obsJobsByCompositeId.set(`${obsJob.source_id}\u0000${obsJob.id}`, obsJob);
  }

  // Discover all valid four-part chats across the normalized WorkspaceSnapshot.
  // Incomplete identities are withheld and never attached by title, project_label, sessionId, or job_id.
  const accumulators = new Map<string, DiscoveredChatAccumulator>();
  const ensureChat = (identity: WorkspaceChatIdentity): DiscoveredChatAccumulator | null => {
    const chatKey = formatWorkspaceChatIdentityKey(identity);
    if (chatKey === null) {
      return null;
    }
    let acc = accumulators.get(chatKey);
    if (!acc) {
      const valid = validateWorkspaceChatIdentity(identity);
      acc = {
        chatKey,
        identity: valid,
        chatSummary: null,
        nativeSample: null,
        jobHistories: [],
        messageItems: [],
        activityRequests: [],
        obsJobs: [],
      };
      accumulators.set(chatKey, acc);
    }
    return acc;
  };

  for (const chatSummary of ws.chats) {
    if (isValidWorkspaceChatIdentity(chatSummary.identity)) {
      const acc = ensureChat(chatSummary.identity);
      if (acc) {
        acc.chatSummary = chatSummary;
      }
    }
  }

  for (const sample of ws.native_samples) {
    if (isValidWorkspaceChatIdentity(sample.chat_identity)) {
      const acc = ensureChat(sample.chat_identity);
      if (acc) {
        acc.nativeSample = sample;
      }
    }
  }

  const unscopedJobsBySource = new Map<string, number>();
  for (const history of ws.job_histories) {
    if (
      history.job.chat_identity !== null &&
      isValidWorkspaceChatIdentity(history.job.chat_identity)
    ) {
      const acc = ensureChat(history.job.chat_identity);
      if (acc) {
        acc.jobHistories.push(history);
      }
    } else {
      unscopedJobsBySource.set(
        history.job.source_id,
        (unscopedJobsBySource.get(history.job.source_id) ?? 0) + 1,
      );
    }
  }

  const knownScopes = collectKnownScopes(ws);

  for (const obsJob of ws.observation.jobs) {
    if (
      typeof obsJob.binding_id === 'string' &&
      typeof obsJob.thread_uuid === 'string'
    ) {
      let scopeCandidate: string | null =
        typeof obsJob.project_path === 'string' && obsJob.project_path.length > 0
          ? obsJob.project_path
          : null;
      if (scopeCandidate === null) {
        const tripleKey = `${obsJob.source_id}\u0000${obsJob.binding_id}\u0000${obsJob.thread_uuid}`;
        const tripleScopes = knownScopes.scopesByTriple.get(tripleKey);
        if (tripleScopes && tripleScopes.size > 1) {
          scopeCandidate = null;
        } else if (tripleScopes && tripleScopes.size === 1) {
          scopeCandidate = Array.from(tripleScopes)[0] ?? null;
        } else {
          const sourceScopes = knownScopes.scopesBySource.get(obsJob.source_id);
          if (sourceScopes && sourceScopes.size === 1) {
            scopeCandidate = Array.from(sourceScopes)[0] ?? null;
          }
        }
      }
      if (scopeCandidate !== null) {
        const validIdentity = tryValidateChatIdentity({
          source_id: obsJob.source_id,
          binding_id: obsJob.binding_id,
          thread_uuid: obsJob.thread_uuid,
          canonical_project_scope: scopeCandidate,
        });
        if (validIdentity !== null) {
          const acc = ensureChat(validIdentity);
          if (acc) {
            acc.obsJobs.push(obsJob);
          }
        }
      }
    }
  }

  const unscopedMessagesBySource = new Map<string, number>();
  for (const item of ws.messages.items) {
    if (
      item.scope.chat_identity !== null &&
      isValidWorkspaceChatIdentity(item.scope.chat_identity)
    ) {
      const acc = ensureChat(item.scope.chat_identity);
      if (acc) {
        acc.messageItems.push(item);
      }
    } else {
      unscopedMessagesBySource.set(
        item.scope.source_id,
        (unscopedMessagesBySource.get(item.scope.source_id) ?? 0) + 1,
      );
    }
  }

  const unscopedRequestsBySource = new Map<string, number>();
  for (const req of ws.observation.requests) {
    const resolvedIdentity = resolveObservationRequestChatIdentityWithScopes(
      req,
      ws,
      knownScopes,
    );
    if (resolvedIdentity !== null) {
      const acc = ensureChat(resolvedIdentity);
      if (acc) {
        acc.activityRequests.push(req);
      }
    } else {
      unscopedRequestsBySource.set(
        req.source_id,
        (unscopedRequestsBySource.get(req.source_id) ?? 0) + 1,
      );
    }
  }

  const diagnostics = projectDiagnostics(ws.observation);
  const boundsNotices = buildSnapshotBoundsNotices(ws);

  const sidebarByKey = new Map<string, WorkspaceChatSidebarItem>();
  const projectedJobsByChatKey = new Map<string, WorkspaceJobView[]>();
  const projectedActivityByChatKey = new Map<string, WorkspaceActivityItemView[]>();
  const activeChats: WorkspaceChatSidebarItem[] = [];
  const archivedChats: WorkspaceChatSidebarItem[] = [];

  for (const acc of accumulators.values()) {
    const { identity, chatKey } = acc;
    const lifecycle = resolveWorkspaceChatLifecycle(archivePreferences, identity);
    const projectKey = formatWorkspaceProjectGroupKey(
      identity.source_id,
      identity.canonical_project_scope,
    );

    // Resolve display labels strictly after canonical 4-part identity grouping;
    // preserve agent/user content verbatim in its source language.
    let rawThreadLabel: string | null = null;
    if (acc.chatSummary && !isDefaultChatPlaceholderLabel(acc.chatSummary.thread_label)) {
      rawThreadLabel = acc.chatSummary.thread_label;
    }
    if (rawThreadLabel === null) {
      const labeledJob = acc.obsJobs.find(
        (j) => !isDefaultChatPlaceholderLabel(j.thread_label),
      );
      if (labeledJob?.thread_label) {
        rawThreadLabel = labeledJob.thread_label;
      }
    }
    if (rawThreadLabel === null) {
      const labeledReq = acc.activityRequests.find(
        (r) => !isDefaultChatPlaceholderLabel(r.thread_label),
      );
      if (labeledReq?.thread_label) {
        rawThreadLabel = labeledReq.thread_label;
      }
    }
    const threadLabel = sanitizeWorkspaceLabel(
      rawThreadLabel ?? acc.chatSummary?.thread_label ?? 'Codex chat',
      'Codex chat',
    );

    let rawProjectLabel: string | null = null;
    if (
      acc.chatSummary &&
      !isDefaultProjectPlaceholderLabel(acc.chatSummary.project_label)
    ) {
      rawProjectLabel = acc.chatSummary.project_label;
    }
    if (rawProjectLabel === null) {
      const labeledJob = acc.obsJobs.find(
        (j) => !isDefaultProjectPlaceholderLabel(j.project_label),
      );
      if (labeledJob?.project_label) {
        rawProjectLabel = labeledJob.project_label;
      }
    }
    if (rawProjectLabel === null) {
      const src = sourcesById.get(identity.source_id);
      if (
        src &&
        (src.project_path === identity.canonical_project_scope || src.project_path === null) &&
        !isDefaultProjectPlaceholderLabel(src.label)
      ) {
        rawProjectLabel = src.label;
      }
    }
    if (rawProjectLabel === null) {
      rawProjectLabel = projectScopeBasename(identity.canonical_project_scope);
    }
    const projectLabel = sanitizeWorkspaceLabel(rawProjectLabel, 'Project');

    const { nativeState, configuredSelection } = projectNativeChatStateAndSelection(
      acc.nativeSample,
      acc.chatSummary,
      referenceNowMs,
      ws.generated_at_ms,
      ws.sampling_policy.stale_threshold_ms,
    );

    const jobViews = acc.jobHistories.map((h) =>
      projectWorkspaceJobView(h, identity, referenceNowMs),
    );
    projectedJobsByChatKey.set(chatKey, jobViews);

    const orderedRequests = [...acc.activityRequests].sort((left, right) => {
      if (right.updated_at_ms !== left.updated_at_ms) {
        return right.updated_at_ms - left.updated_at_ms;
      }
      return compareUtf8Lexical(left.id, right.id);
    });
    const activityItems = orderedRequests.map((req) =>
      projectWorkspaceActivityItemView(req, identity, referenceNowMs),
    );
    projectedActivityByChatKey.set(chatKey, activityItems);

    const workerSummary = computeChatWorkerSummary(
      acc.jobHistories,
      obsJobsByCompositeId,
      activityItems,
    );
    const jobSummary = computeChatJobSummary(
      jobViews,
      acc.chatSummary?.active_job_count ?? 0,
    );

    const blockers = collectChatBlockers(
      chatKey,
      nativeState,
      jobViews,
      activityItems,
      referenceNowMs,
    );
    const needsHumanCount = Math.max(
      blockers.length,
      acc.chatSummary?.attention_count ?? 0,
    );
    const hasCurrentBlocker = needsHumanCount > 0;
    const needsHumanBadgeLabel = formatNeedsHumanBadge(needsHumanCount);

    let updatedAtMs: number | null = acc.chatSummary?.updated_at_ms ?? null;
    const considerTs = (ts: number | null | undefined) => {
      if (typeof ts === 'number' && Number.isFinite(ts) && ts > 0) {
        if (updatedAtMs === null || ts > updatedAtMs) {
          updatedAtMs = ts;
        }
      }
    };
    considerTs(acc.nativeSample?.observed_at_ms);
    for (const jv of jobViews) {
      considerTs(jv.updatedAtMs);
    }
    for (const msg of acc.messageItems) {
      considerTs(msg.source_order.recorded_at_ms);
    }
    for (const act of activityItems) {
      considerTs(act.updatedAtMs);
    }

    let primaryDotTone: StatusDotTone = nativeState.tone;
    let statusSummaryLabel: string;
    if (hasCurrentBlocker) {
      primaryDotTone = 'attention';
      statusSummaryLabel =
        blockers[0]?.kindLabel ?? needsHumanBadgeLabel ?? 'Needs attention';
    } else if (
      nativeState.availability === 'available' &&
      nativeState.activity === 'working' &&
      workerSummary.hasActiveWorkers
    ) {
      primaryDotTone = 'working';
      statusSummaryLabel = `${nativeState.activityLabel} · ${workerSummary.statusLabel}`;
    } else if (
      nativeState.availability === 'available' &&
      nativeState.activity === 'working'
    ) {
      primaryDotTone = 'working';
      statusSummaryLabel = nativeState.activityLabel;
    } else if (workerSummary.hasActiveWorkers) {
      primaryDotTone = 'working';
      statusSummaryLabel =
        nativeState.availability === 'available'
          ? `${nativeState.activityLabel} · ${workerSummary.statusLabel}`
          : workerSummary.statusLabel;
    } else if (nativeState.availability === 'available') {
      primaryDotTone = nativeState.tone;
      statusSummaryLabel = nativeState.activityLabel;
    } else if (nativeState.availability === 'stale') {
      primaryDotTone = 'neutral';
      statusSummaryLabel = 'Native state stale';
    } else if (jobSummary.totalJobCount > 0) {
      primaryDotTone =
        jobSummary.failedJobCount > 0 && jobSummary.verifiedAcceptedJobCount === 0
          ? 'danger'
          : 'neutral';
      statusSummaryLabel = jobSummary.statusLabel;
    } else {
      primaryDotTone = 'neutral';
      statusSummaryLabel = nativeState.availabilityLabel;
    }

    const targetJobId =
      blockers.find((b) => b.jobId !== null)?.jobId ?? jobViews[0]?.jobId ?? null;

    const sidebarItem: WorkspaceChatSidebarItem = {
      chatKey,
      identity,
      lifecycle,
      projectKey,
      projectLabel,
      canonicalProjectScope: identity.canonical_project_scope,
      threadLabel,
      updatedAtMs,
      ageLabel: formatAge(updatedAtMs, referenceNowMs),
      nativeState,
      configuredSelection,
      workerSummary,
      jobSummary,
      needsHumanCount,
      hasCurrentBlocker,
      needsHumanBadgeLabel,
      blockers,
      primaryDotTone,
      statusSummaryLabel,
      canArchive: lifecycle === 'active',
      canRestore: lifecycle === 'archived',
      scopedTarget: {
        identity,
        job_id: targetJobId,
      },
      codexTarget: {
        source_id: identity.source_id,
        thread_uuid: identity.thread_uuid,
        job_id: targetJobId,
      },
    };

    sidebarByKey.set(chatKey, sidebarItem);
    if (lifecycle === 'archived') {
      archivedChats.push(sidebarItem);
    } else {
      activeChats.push(sidebarItem);
    }
  }

  const activeProjects = buildWorkspaceProjectGroups(
    activeChats,
    'active',
    referenceNowMs,
  );
  const archivedProjects = buildWorkspaceProjectGroups(
    archivedChats,
    'archived',
    referenceNowMs,
  );
  const visibleProjects =
    lifecycleFilter === 'archived' ? archivedProjects : activeProjects;

  let activeNeedsHumanCount = 0;
  let archivedNeedsHumanCount = 0;
  let totalWorkingCoworkerCount = 0;
  let totalActiveWorkerCount = 0;

  for (const chat of activeChats) {
    activeNeedsHumanCount += chat.needsHumanCount;
    totalActiveWorkerCount += chat.workerSummary.activeWorkerCount;
    if (
      chat.nativeState.availability === 'available' &&
      chat.nativeState.activity === 'working'
    ) {
      totalWorkingCoworkerCount += 1;
    }
  }

  const archivedBlockedChats: WorkspaceArchivedBlockerSummary[] = [];
  for (const chat of [...archivedChats].sort(compareWorkspaceChatSidebarItems)) {
    archivedNeedsHumanCount += chat.needsHumanCount;
    totalActiveWorkerCount += chat.workerSummary.activeWorkerCount;
    if (
      chat.nativeState.availability === 'available' &&
      chat.nativeState.activity === 'working'
    ) {
      totalWorkingCoworkerCount += 1;
    }
    if (chat.hasCurrentBlocker) {
      archivedBlockedChats.push({
        chatKey: chat.chatKey,
        identity: chat.identity,
        threadLabel: chat.threadLabel,
        projectLabel: chat.projectLabel,
        needsHumanCount: chat.needsHumanCount,
        primaryReason:
          chat.blockers[0]?.reason ??
          'Archived chat still has an active blocker requiring human attention.',
        nextStep: chat.blockers[0]?.nextStep ?? 'Restore or open this chat in Projects/Archive.',
        updatedAtMs: chat.blockers[0]?.updatedAtMs ?? chat.updatedAtMs,
      });
    }
  }

  const totalNeedsHumanCount = activeNeedsHumanCount + archivedNeedsHumanCount;
  const archivedBlockerCount = archivedNeedsHumanCount;
  const archivedBlockerNotice =
    archivedBlockerCount > 0
      ? archivedBlockedChats.length === 1
        ? `1 archived chat ("${archivedBlockedChats[0]!.threadLabel}") still has ${archivedBlockerCount} active blocker(s) in Projects/Archive.`
        : `${archivedBlockedChats.length} archived chats still have ${archivedBlockerCount} active blocker(s) in Projects/Archive.`
      : null;

  const statusNotices: string[] = [];
  if (error !== null) {
    statusNotices.push(
      `Showing last known workspace snapshot because the latest refresh failed: ${error}`,
    );
  }
  for (const unavailable of unavailableSources) {
    statusNotices.push(unavailable.statusText);
  }
  if (archivedBlockerNotice !== null) {
    statusNotices.push(archivedBlockerNotice);
  }
  for (const boundNotice of boundsNotices) {
    statusNotices.push(boundNotice);
  }

  const totalChatCount = activeChats.length + archivedChats.length;
  const allSourcesUnavailable =
    ws.observation.sources.length > 0 &&
    ws.observation.sources.every((s) => s.availability !== 'available') &&
    totalChatCount === 0;

  if (allSourcesUnavailable) {
    const primaryReason =
      unavailableSources[0]?.statusText ??
      error ??
      'Linked workspace observation sources are currently unavailable.';
    return {
      status: 'unavailable',
      unavailableReason: primaryReason,
      emptyMessage: null,
      isStale: Boolean(error),
      staleReason: error,
      referenceNowMs,
      lifecycleFilter,
      surfaceSeparation: WORKSPACE_SURFACE_SEPARATION,
      availableTabs: WORKSPACE_CHAT_SURFACE_TABS,
      defaultTab: DEFAULT_WORKSPACE_CHAT_TAB,
      activeProjects,
      archivedProjects,
      visibleProjects,
      activeChatCount: activeChats.length,
      archivedChatCount: archivedChats.length,
      totalWorkingCoworkerCount,
      totalActiveWorkerCount,
      totalNeedsHumanCount,
      activeNeedsHumanCount,
      archivedNeedsHumanCount,
      archivedBlockerCount,
      archivedBlockedChats,
      archivedBlockerNotice,
      unavailableSourceCount: unavailableSources.length,
      unavailableSources,
      bounds: { ...ws.bounds },
      boundsNotices,
      statusNotices,
      selectedChatKey: options.selectedChatKey ?? null,
      selectedChat: null,
      warnings: [...ws.observation.warnings],
      diagnostics,
    };
  }

  const visibleChatsFlat = visibleProjects.flatMap((g) => g.chats);
  const requestedChatKey = options.selectedChatKey ?? null;
  let resolvedChatKey: string | null = null;
  if (requestedChatKey !== null && sidebarByKey.has(requestedChatKey)) {
    resolvedChatKey = requestedChatKey;
  } else if (visibleChatsFlat.length > 0) {
    resolvedChatKey = visibleChatsFlat[0]!.chatKey;
  } else {
    resolvedChatKey = null;
  }

  let selectedChat: WorkspaceSelectedChatProjection | null = null;
  if (resolvedChatKey !== null) {
    const selectedSidebar = sidebarByKey.get(resolvedChatKey) ?? null;
    const selectedAcc = accumulators.get(resolvedChatKey) ?? null;
    if (selectedSidebar && selectedAcc) {
      const sourceId = selectedSidebar.identity.source_id;

      // 1. Messages surface (default tab; strictly 4-part scoped)
      const messageViews = selectedAcc.messageItems.map((m) =>
        projectMessageItemView(m, referenceNowMs),
      );
      const unscopedMessagesCount =
        (unscopedMessagesBySource.get(sourceId) ?? 0) +
        (sourceId !== 'unknown' ? (unscopedMessagesBySource.get('unknown') ?? 0) : 0);
      const messagesStatusNotices: string[] = [];
      if (ws.messages.availability === 'stale') {
        messagesStatusNotices.push(
          'Message summaries window is stale; recent safe summaries may be delayed.',
        );
      } else if (ws.messages.availability === 'disconnected') {
        messagesStatusNotices.push('Message summaries route is disconnected.');
      } else if (ws.messages.availability === 'unavailable') {
        messagesStatusNotices.push(
          'Safe message summaries are unavailable for this snapshot.',
        );
      }
      if (ws.messages.omitted_count > 0) {
        messagesStatusNotices.push(
          `Showing a bounded window of up to ${ws.messages.max_items} message summaries; ${ws.messages.omitted_count} summary item(s) were omitted.`,
        );
      } else if (ws.messages.truncated) {
        messagesStatusNotices.push('Message summaries window was truncated.');
      }
      if (ws.messages.has_more) {
        messagesStatusNotices.push(
          'Additional message summaries exist beyond this bounded window.',
        );
      }
      if (unscopedMessagesCount > 0) {
        messagesStatusNotices.push(
          `${unscopedMessagesCount} message summary record(s) in source "${sourceId}" lacked a verified four-part chat identity and were withheld instead of matching by title or job ID.`,
        );
      }

      const messagesSurface: WorkspaceChatMessagesSurface = {
        kind: WORKSPACE_SURFACE_SEPARATION.messages_surface.kind,
        availability: ws.messages.availability,
        availabilityLabel: formatWorkspaceAvailabilityLabel(ws.messages.availability),
        items: messageViews,
        maxItems: ws.messages.max_items,
        omittedCount: ws.messages.omitted_count,
        hasMore: ws.messages.has_more,
        truncated: ws.messages.truncated,
        unscopedWithheldCount: unscopedMessagesCount,
        requiresTranscript: false,
        allowsSyntheticUserMessages: false,
        emptyMessage:
          messageViews.length === 0
            ? ws.messages.availability === 'available'
              ? 'No safe message summaries recorded for this chat yet.'
              : 'Safe message summaries are currently unavailable for this chat.'
            : null,
        statusNotices: messagesStatusNotices,
      };

      // 2. Jobs surface (separate durable ledger; strictly 4-part scoped)
      const jobViews = projectedJobsByChatKey.get(resolvedChatKey) ?? [];
      const unscopedJobsCount =
        (unscopedJobsBySource.get(sourceId) ?? 0) +
        (sourceId !== 'unknown' ? (unscopedJobsBySource.get('unknown') ?? 0) : 0);
      const jobsStatusNotices: string[] = [];
      if (ws.bounds.job_histories_omitted > 0) {
        jobsStatusNotices.push(
          `Showing a bounded job history window; ${ws.bounds.job_histories_omitted} job history record(s) were omitted.`,
        );
      }
      if (ws.observation.jobs_has_more || ws.bounds.jobs_capped) {
        jobsStatusNotices.push(
          'More jobs exist beyond this bounded snapshot window (jobs_has_more).',
        );
      }
      for (const jv of jobViews) {
        for (const tNotice of jv.truncationNotices) {
          jobsStatusNotices.push(`Job "${jv.title}" (${jv.jobId}): ${tNotice}`);
        }
      }
      if (unscopedJobsCount > 0) {
        jobsStatusNotices.push(
          `${unscopedJobsCount} job record(s) in source "${sourceId}" lacked a verified four-part chat identity and were withheld.`,
        );
      }

      const jobsSurface: WorkspaceChatJobsSurface = {
        kind: WORKSPACE_SURFACE_SEPARATION.jobs_surface.kind,
        items: jobViews,
        totalJobCount: jobViews.length,
        activeJobCount: selectedSidebar.jobSummary.activeJobCount,
        verifiedAcceptedJobCount: selectedSidebar.jobSummary.verifiedAcceptedJobCount,
        historicalFailureJobCount: jobViews.filter(
          (j) =>
            j.historicalFailure !== null ||
            j.tasks.some((t) => t.historicalFailure !== null),
        ).length,
        currentAttentionJobCount: selectedSidebar.jobSummary.needsAttentionJobCount,
        jobHistoriesOmitted: ws.bounds.job_histories_omitted,
        jobsHasMore: ws.observation.jobs_has_more,
        jobsCapped: ws.bounds.jobs_capped,
        unscopedWithheldCount: unscopedJobsCount,
        emptyMessage:
          jobViews.length === 0 ? 'No jobs recorded for this chat.' : null,
        statusNotices: jobsStatusNotices,
      };

      // 3. Activity surface (separate audit feed; strictly 4-part scoped, optional job sub-filter)
      const allScopedActivity = projectedActivityByChatKey.get(resolvedChatKey) ?? [];
      const cleanJobFilter = validateCorrelationId(options.activityJobId ?? null);
      const visibleActivity =
        cleanJobFilter !== null
          ? allScopedActivity.filter((item) => item.jobId === cleanJobFilter)
          : allScopedActivity;
      const unscopedRequestsCount =
        (unscopedRequestsBySource.get(sourceId) ?? 0) +
        (sourceId !== 'unknown' ? (unscopedRequestsBySource.get('unknown') ?? 0) : 0) +
        (sourceId !== '' ? (unscopedRequestsBySource.get('') ?? 0) : 0);
      const activityStatusNotices: string[] = [];
      if (ws.observation.has_more || ws.bounds.requests_capped) {
        activityStatusNotices.push(
          `Showing a bounded activity request window${ws.bounds.requests_capped ? ' (page limit reached)' : ''}; older activity items exist beyond this page.`,
        );
      }
      for (const act of visibleActivity) {
        if (act.truncationNotice !== null) {
          activityStatusNotices.push(
            `Request "${act.title}" (${act.requestId}): ${act.truncationNotice}`,
          );
        }
      }
      if (unscopedRequestsCount > 0) {
        activityStatusNotices.push(
          `${unscopedRequestsCount} activity request(s) in source "${sourceId}" lacked a verified four-part chat identity and were withheld instead of matching by title or job ID.`,
        );
      }

      const activitySurface: WorkspaceChatActivitySurface = {
        kind: WORKSPACE_SURFACE_SEPARATION.activity_surface.kind,
        items: visibleActivity,
        totalScopedCount: allScopedActivity.length,
        activeJobFilter: cleanJobFilter,
        runningCount: visibleActivity.filter((a) => a.running).length,
        hasMore: ws.observation.has_more,
        requestsCapped: ws.bounds.requests_capped,
        nextCursor: ws.observation.next_cursor,
        unscopedWithheldCount: unscopedRequestsCount,
        emptyMessage:
          visibleActivity.length === 0
            ? cleanJobFilter !== null
              ? `No activity requests recorded for job ${cleanJobFilter} in this chat.`
              : 'No activity requests recorded for this chat in the current window.'
            : null,
        statusNotices: activityStatusNotices,
      };

      const chatStatusNotices: string[] = [];
      if (selectedSidebar.nativeState.statusNotice !== null) {
        chatStatusNotices.push(selectedSidebar.nativeState.statusNotice);
      }
      const selectedUnavailableSource = unavailableSources.find(
        (u) => u.sourceId === sourceId,
      );
      if (selectedUnavailableSource) {
        chatStatusNotices.push(selectedUnavailableSource.statusText);
      }
      if (
        selectedSidebar.lifecycle === 'archived' &&
        selectedSidebar.hasCurrentBlocker
      ) {
        chatStatusNotices.push(
          'This chat is currently in Projects/Archive (view preference only) and still has active human blocker(s).',
        );
      }

      const chatDiagnostics = diagnostics.filter(
        (d) => d.sourceId === null || d.sourceId === sourceId,
      );

      selectedChat = {
        chatKey: selectedSidebar.chatKey,
        identity: selectedSidebar.identity,
        lifecycle: selectedSidebar.lifecycle,
        projectKey: selectedSidebar.projectKey,
        projectLabel: selectedSidebar.projectLabel,
        canonicalProjectScope: selectedSidebar.canonicalProjectScope,
        threadLabel: selectedSidebar.threadLabel,
        updatedAtMs: selectedSidebar.updatedAtMs,
        ageLabel: selectedSidebar.ageLabel,
        activeTab: requestedTab,
        availableTabs: WORKSPACE_CHAT_SURFACE_TABS,
        nativeState: selectedSidebar.nativeState,
        configuredSelection: selectedSidebar.configuredSelection,
        workerSummary: selectedSidebar.workerSummary,
        jobSummary: selectedSidebar.jobSummary,
        needsHumanCount: selectedSidebar.needsHumanCount,
        hasCurrentBlocker: selectedSidebar.hasCurrentBlocker,
        blockers: selectedSidebar.blockers,
        primaryBlocker: selectedSidebar.blockers[0] ?? null,
        scopedTarget: selectedSidebar.scopedTarget,
        codexTarget: selectedSidebar.codexTarget,
        statusNotices: chatStatusNotices,
        diagnostics: chatDiagnostics,
        messages: messagesSurface,
        jobs: jobsSurface,
        activity: activitySurface,
      };
    }
  }

  const status: WorkspaceProductionProjection['status'] =
    totalChatCount === 0 ? 'empty' : 'ready';

  let emptyMessage: string | null = null;
  if (totalChatCount === 0) {
    emptyMessage = 'No chats recorded yet.';
  } else if (visibleProjects.length === 0) {
    emptyMessage =
      lifecycleFilter === 'archived'
        ? 'No archived chats in Projects/Archive.'
        : 'No chats in Projects/Active. Archived chats remain available in Projects/Archive.';
  }

  return {
    status,
    unavailableReason: null,
    emptyMessage,
    isStale: Boolean(error),
    staleReason: error,
    referenceNowMs,
    lifecycleFilter,
    surfaceSeparation: WORKSPACE_SURFACE_SEPARATION,
    availableTabs: WORKSPACE_CHAT_SURFACE_TABS,
    defaultTab: DEFAULT_WORKSPACE_CHAT_TAB,
    activeProjects,
    archivedProjects,
    visibleProjects,
    activeChatCount: activeChats.length,
    archivedChatCount: archivedChats.length,
    totalWorkingCoworkerCount,
    totalActiveWorkerCount,
    totalNeedsHumanCount,
    activeNeedsHumanCount,
    archivedNeedsHumanCount,
    archivedBlockerCount,
    archivedBlockedChats,
    archivedBlockerNotice,
    unavailableSourceCount: unavailableSources.length,
    unavailableSources,
    bounds: { ...ws.bounds },
    boundsNotices,
    statusNotices,
    selectedChatKey: resolvedChatKey,
    selectedChat,
    warnings: [...ws.observation.warnings],
    diagnostics,
  };
}
