/**
 * Pure projection, preference normalization, and async request coordination
 * for the Threadlane system tray popover and window-close preference controls.
 *
 * This module has no DOM, CSS, SVG, or Tauri runtime imports so it can be
 * validated directly under `node --experimental-strip-types --test`.
 */
import {
  WORKSPACE_MAIN_CLOSE_BEHAVIORS,
  WORKSPACE_TRAY_ACTIONS,
  WORKSPACE_TRAY_MAX_DETAIL_CHARS,
  WORKSPACE_TRAY_MAX_ERROR_CHARS,
  WORKSPACE_TRAY_MAX_ROWS,
  WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS,
  WORKSPACE_TRAY_REFRESH_INTERVAL_MS,
  WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT,
  boundWorkspaceCompactTrayRows,
  type CodexChatTarget,
  type WorkspaceChatArchivePreferenceMap,
  type WorkspaceChatIdentity,
  type WorkspaceChatPreferenceStorage,
  type WorkspaceCompactTrayRow,
  type WorkspaceMainCloseBehavior,
  type WorkspaceSnapshot,
  type WorkspaceTrayAction,
  type WorkspaceTrayRowKind,
} from './app-contract.ts';
import {
  isCanonicalUuid,
  projectWorkspaceSnapshot,
  type WorkspaceChatSidebarItem,
  type WorkspaceJobView,
  type WorkspaceSelectedChatProjection,
} from './app-model.ts';
import type {
  ObservationJob,
  ObservationSnapshotRequest,
  WorkspaceAttentionKind,
} from './observation/types.ts';
import { formatRelativeTime } from './pages/activity-feed-model.ts';
import {
  normalizeWorkspaceSnapshot,
  sanitizeWorkspaceSummary,
  validateCorrelationId,
} from './workspace-contract.ts';

export const TRAY_WINDOW_LABEL = 'tray' as const;
export const TRAY_WINDOW_URL = 'index.html?surface=tray' as const;
export const TRAY_OPEN_EVENT = 'threadlane:tray-opened' as const;
export const TRAY_PREFERENCES_EVENT = 'threadlane:tray-preferences-changed' as const;
export const TRAY_GET_PREFERENCES_COMMAND = 'desktop_tray_preferences' as const;
export const TRAY_SET_PREFERENCES_COMMAND = 'desktop_set_tray_preferences' as const;
export const TRAY_ACTION_COMMAND = 'desktop_tray_action' as const;
export const DEFAULT_TRAY_CLOSE_BEHAVIOR: TrayCloseBehavior = 'hide_to_tray';

export const TRAY_MAX_ROWS = WORKSPACE_TRAY_MAX_ROWS;
export const TRAY_MAX_TERMINAL_RESULT_ROWS = WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS;
export const TRAY_SNAPSHOT_PAGE_LIMIT = WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT;
export const TRAY_REFRESH_INTERVAL_MS = WORKSPACE_TRAY_REFRESH_INTERVAL_MS;

export type TrayCloseBehavior = WorkspaceMainCloseBehavior;
export type TrayAction = WorkspaceTrayAction;

export interface SetTrayPreferencesArgs {
  request: {
    close_behavior: TrayCloseBehavior;
  };
}

export interface TrayPreferencesResponse {
  close_behavior: TrayCloseBehavior;
  tray_available: boolean;
  message: string | null;
}

export interface TrayActionArgs {
  action: TrayAction;
}

export type TrayPreferencesAvailability =
  | 'loading'
  | 'available'
  | 'unavailable';

export interface TrayPreferencesState {
  availability: TrayPreferencesAvailability;
  closeBehavior: TrayCloseBehavior;
  saving: boolean;
  message: string | null;
  error: string | null;
}

export type TrayStatusKind = 'loading' | 'unavailable' | 'empty' | 'ready';
export type TrayRowCategory = WorkspaceTrayRowKind;
export type TrayRowTone =
  | 'attention'
  | 'working'
  | 'success'
  | 'danger'
  | 'unknown'
  | 'neutral';

export type TrayCoworkerTone =
  | 'attention'
  | 'working'
  | 'idle'
  | 'stale'
  | 'disconnected'
  | 'unavailable';

export interface TrayCoworkerSummary {
  tone: TrayCoworkerTone;
  label: string;
  workingCount: number;
  waitingCount: number;
  idleCount: number;
  staleCount: number;
  disconnectedCount: number;
  unavailableCount: number;
  oldestStaleAgeMs: number | null;
}

export interface TrayRowView {
  key: string;
  category: TrayRowCategory;
  tone: TrayRowTone;
  title: string;
  projectLabel: string;
  chatLabel: string;
  statusLabel: string;
  coworkerLabel: string | null;
  coworkerTone: TrayCoworkerTone | null;
  detail: string;
  updatedAtMs: number | null;
  ageLabel: string;
  chatIdentity: WorkspaceChatIdentity | null;
  threadUuid: string | null;
  jobId: string | null;
  codexTarget: CodexChatTarget | null;
}

export interface TrayProjection {
  status: TrayStatusKind;
  headerStatusLabel: string;
  workingCount: number | null;
  attentionCount: number | null;
  pendingAcceptanceCount: number;
  unavailableSourceCount: number;
  isStale: boolean;
  refreshError: string | null;
  sourceNotice: string | null;
  coworkerSummary: TrayCoworkerSummary | null;
  rows: TrayRowView[];
  emptyMessage: string | null;
  unavailableReason: string | null;
}

export interface TrayProjectionOptions {
  loading?: boolean;
  error?: string | null;
  nowMs?: number;
  archivePreferences?: WorkspaceChatArchivePreferenceMap;
  preferenceStorage?: Pick<WorkspaceChatPreferenceStorage, 'getItem'> | null;
}

const ATTENTION_KIND_PRIORITY: Record<WorkspaceAttentionKind, number> = {
  on_hold: 0,
  unresolved_effect: 1,
  needs_approval: 2,
  needs_user_input: 3,
  blocked_failure: 4,
  review_required: 5,
  unknown: 6,
  none: 7,
};

function trimOrNull(value: string | null | undefined): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
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

function truncateCodePoints(raw: string, maxChars: number): string {
  const sanitized = sanitizeWorkspaceSummary(raw);
  const codePoints = Array.from(sanitized);
  if (codePoints.length <= maxChars) {
    return sanitized;
  }
  const keep = Math.max(0, maxChars - 1);
  return `${codePoints.slice(0, keep).join('')}…`;
}

function formatCompactAgeMs(ms: number): string {
  const safe = Math.max(0, Math.trunc(ms));
  if (safe < 1_000) {
    return `${safe}ms`;
  }
  const seconds = Math.round(safe / 1_000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remSeconds = seconds % 60;
  return remSeconds > 0 ? `${minutes}m ${remSeconds}s` : `${minutes}m`;
}

function formatAgeLabel(atMs: number | null, nowMs: number): string {
  if (atMs === null) {
    return 'Time not reported';
  }
  return formatRelativeTime(atMs, nowMs);
}

function isForbiddenIdentitySubstitute(raw: string): boolean {
  const lower = raw.toLowerCase();
  return (
    lower.startsWith('job_') ||
    lower.startsWith('job-') ||
    lower.startsWith('job:') ||
    lower.startsWith('plan_') ||
    lower.startsWith('plan-') ||
    lower.startsWith('plan:') ||
    lower.startsWith('sess_') ||
    lower.startsWith('sess-') ||
    lower.startsWith('sess:') ||
    lower.startsWith('session_') ||
    lower.startsWith('session-') ||
    lower.startsWith('session:') ||
    lower.startsWith('sessionid')
  );
}

export function validateTrayCodexChatTarget(
  raw: unknown
): CodexChatTarget | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return null;
  }
  const record = raw as Record<string, unknown>;
  for (const key of Object.keys(record)) {
    if (key !== 'source_id' && key !== 'thread_uuid' && key !== 'job_id') {
      return null;
    }
  }

  if (typeof record.source_id !== 'string') {
    return null;
  }
  const cleanSourceId = validateCorrelationId(record.source_id);
  if (
    cleanSourceId === null ||
    cleanSourceId !== record.source_id ||
    !/^[A-Za-z0-9_.-]+$/.test(cleanSourceId) ||
    isForbiddenIdentitySubstitute(cleanSourceId)
  ) {
    return null;
  }

  if (
    typeof record.thread_uuid !== 'string' ||
    !isCanonicalUuid(record.thread_uuid)
  ) {
    return null;
  }

  let cleanJobId: string | null = null;
  if (record.job_id !== null && record.job_id !== undefined) {
    if (typeof record.job_id !== 'string') {
      return null;
    }
    const validatedJobId = validateCorrelationId(record.job_id);
    if (validatedJobId === null || validatedJobId !== record.job_id) {
      return null;
    }
    cleanJobId = validatedJobId;
  }

  return {
    source_id: cleanSourceId,
    thread_uuid: record.thread_uuid,
    job_id: cleanJobId,
  };
}

export function isTrayCloseBehavior(value: unknown): value is TrayCloseBehavior {
  return (WORKSPACE_MAIN_CLOSE_BEHAVIORS as readonly unknown[]).includes(value);
}

export function isTrayAction(value: unknown): value is TrayAction {
  return (WORKSPACE_TRAY_ACTIONS as readonly unknown[]).includes(value);
}

export function createInitialTrayPreferencesState(
  hasRuntime: boolean = true
): TrayPreferencesState {
  if (!hasRuntime) {
    return {
      availability: 'unavailable',
      closeBehavior: DEFAULT_TRAY_CLOSE_BEHAVIOR,
      saving: false,
      message: 'System tray preferences require the native desktop app.',
      error: null,
    };
  }
  return {
    availability: 'loading',
    closeBehavior: DEFAULT_TRAY_CLOSE_BEHAVIOR,
    saving: false,
    message: null,
    error: null,
  };
}

export function normalizeTrayErrorMessage(
  err: unknown,
  fallback: string = 'System tray request failed.'
): string {
  const raw =
    typeof err === 'string'
      ? err
      : err &&
          typeof err === 'object' &&
          'message' in err &&
          typeof (err as { message: unknown }).message === 'string'
        ? (err as { message: string }).message
        : fallback;
  const cleaned = raw
    .replace(/bearer\s+[a-z0-9._-]+/gi, '[redacted]')
    .replace(/\bsk-[a-z0-9._-]+/gi, '[redacted]')
    .replace(
      /(api_?key|secret|token|password)\s*[=:]\s*[^\s,;]+/gi,
      '$1=[redacted]'
    )
    .replace(/[\u0000-\u001f\u007f-\u009f]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim();
  if (!cleaned) {
    return fallback;
  }
  const codePoints = Array.from(cleaned);
  if (codePoints.length <= WORKSPACE_TRAY_MAX_ERROR_CHARS) {
    return cleaned;
  }
  const keep = Math.max(0, WORKSPACE_TRAY_MAX_ERROR_CHARS - 1);
  return `${codePoints.slice(0, keep).join('')}…`;
}

export function parseTrayPreferencesResponse(
  raw: unknown
): TrayPreferencesResponse | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return null;
  }
  const record = raw as Record<string, unknown>;
  if (!isTrayCloseBehavior(record.close_behavior)) {
    return null;
  }
  if (typeof record.tray_available !== 'boolean') {
    return null;
  }
  const message =
    record.message === null || record.message === undefined
      ? null
      : typeof record.message === 'string'
        ? trimOrNull(record.message)
        : null;
  return {
    close_behavior: record.close_behavior,
    tray_available: record.tray_available,
    message,
  };
}

export function applyTrayPreferencesLoadSuccess(
  raw: unknown
): TrayPreferencesState {
  const parsed = parseTrayPreferencesResponse(raw);
  if (!parsed) {
    return {
      availability: 'unavailable',
      closeBehavior: DEFAULT_TRAY_CLOSE_BEHAVIOR,
      saving: false,
      message: 'System tray preferences are not available on this build.',
      error: null,
    };
  }
  if (!parsed.tray_available) {
    return {
      availability: 'unavailable',
      closeBehavior: parsed.close_behavior,
      saving: false,
      message:
        parsed.message ??
        'System tray integration is unavailable in this desktop session.',
      error: null,
    };
  }
  return {
    availability: 'available',
    closeBehavior: parsed.close_behavior,
    saving: false,
    message: parsed.message,
    error: null,
  };
}

export function applyTrayPreferencesLoadError(
  err: unknown
): TrayPreferencesState {
  const detail = normalizeTrayErrorMessage(
    err,
    'System tray integration is unavailable.'
  );
  const isMissingSupport =
    /unknown command|not found|unimplemented|unsupported|unavailable|bridge/i.test(
      detail
    );
  return {
    availability: 'unavailable',
    closeBehavior: DEFAULT_TRAY_CLOSE_BEHAVIOR,
    saving: false,
    message: isMissingSupport
      ? 'System tray integration is not available in this environment.'
      : detail,
    error: null,
  };
}

export function applyTrayPreferencesSaveResult(
  previous: TrayPreferencesState,
  raw: unknown
): TrayPreferencesState {
  const parsed = parseTrayPreferencesResponse(raw);
  if (!parsed) {
    return {
      ...previous,
      saving: false,
      error: 'Received an invalid response while saving window close preference.',
    };
  }
  if (!parsed.tray_available) {
    return {
      availability: 'unavailable',
      closeBehavior: parsed.close_behavior,
      saving: false,
      message:
        parsed.message ??
        'System tray integration is unavailable in this desktop session.',
      error: null,
    };
  }
  return {
    availability: 'available',
    closeBehavior: parsed.close_behavior,
    saving: false,
    message: parsed.message,
    error: null,
  };
}

export function applyTrayPreferencesSaveError(
  previous: TrayPreferencesState,
  err: unknown
): TrayPreferencesState {
  return {
    ...previous,
    saving: false,
    error: normalizeTrayErrorMessage(
      err,
      'Failed to save window close preference.'
    ),
  };
}

function formatTrayHeaderSummary(
  workingCount: number,
  attentionCount: number
): string {
  const parts: string[] = [];
  if (workingCount > 0) {
    parts.push(`${workingCount} working`);
  }
  if (attentionCount > 0) {
    parts.push(
      attentionCount === 1 ? '1 needs you' : `${attentionCount} need you`
    );
  }
  if (parts.length === 0) {
    return '0 working · 0 need you';
  }
  return parts.join(' · ');
}

function formatBlockerStatusLabel(
  kind: WorkspaceAttentionKind,
  origin: WorkspaceChatSidebarItem['blockers'][number]['origin'],
  nativeActivity: WorkspaceChatSidebarItem['nativeState']['activity']
): string {
  if (origin === 'native_chat') {
    return nativeActivity === 'waiting_for_approval'
      ? 'Coworker awaiting approval'
      : 'Coworker waiting for input';
  }
  switch (kind) {
    case 'on_hold':
      return 'On hold';
    case 'unresolved_effect':
      return 'Unresolved effect';
    case 'needs_approval':
      return 'Needs approval';
    case 'needs_user_input':
      return origin === 'request_waiting_for_user'
        ? 'Waiting for user'
        : 'Needs user input';
    case 'blocked_failure':
      return 'Blocked by failure';
    case 'review_required':
      return 'Review required';
    case 'unknown':
    case 'none':
      return 'Needs attention';
  }
}

function isActiveWorkerStatusToken(status: string | null | undefined): boolean {
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

interface ChatCoworkerDescriptor {
  tone: TrayCoworkerTone;
  label: string;
  sampleAgeMs: number | null;
}

function describeChatCoworkerState(
  chat: WorkspaceChatSidebarItem,
  hasExplicitNativeRecord: boolean
): ChatCoworkerDescriptor | null {
  const ns = chat.nativeState;
  if (ns.availability === 'available') {
    switch (ns.activity) {
      case 'waiting_for_approval':
        return {
          tone: 'attention',
          label: 'Coworker waiting for approval',
          sampleAgeMs: ns.sampleAgeMs,
        };
      case 'waiting_for_input':
        return {
          tone: 'attention',
          label: 'Coworker waiting for input',
          sampleAgeMs: ns.sampleAgeMs,
        };
      case 'working':
        return {
          tone: 'working',
          label: 'Coworker working',
          sampleAgeMs: ns.sampleAgeMs,
        };
      case 'idle':
        return {
          tone: 'idle',
          label: 'Coworker idle',
          sampleAgeMs: ns.sampleAgeMs,
        };
      case 'unknown':
        return {
          tone: 'unavailable',
          label: 'Coworker activity unknown',
          sampleAgeMs: ns.sampleAgeMs,
        };
    }
  }
  if (ns.availability === 'stale') {
    const agePart =
      ns.sampleAgeMs !== null ? ` (${formatCompactAgeMs(ns.sampleAgeMs)})` : '';
    return {
      tone: 'stale',
      label: `Coworker stale${agePart}`,
      sampleAgeMs: ns.sampleAgeMs,
    };
  }
  if (ns.availability === 'disconnected') {
    return {
      tone: 'disconnected',
      label: 'Coworker disconnected',
      sampleAgeMs: ns.sampleAgeMs,
    };
  }
  if (hasExplicitNativeRecord) {
    return {
      tone: 'unavailable',
      label: 'Coworker unavailable',
      sampleAgeMs: ns.sampleAgeMs,
    };
  }
  return null;
}

function buildAggregateCoworkerSummary(
  descriptors: readonly ChatCoworkerDescriptor[]
): TrayCoworkerSummary | null {
  if (descriptors.length === 0) {
    return null;
  }
  let workingCount = 0;
  let waitingCount = 0;
  let idleCount = 0;
  let staleCount = 0;
  let disconnectedCount = 0;
  let unavailableCount = 0;
  let oldestStaleAgeMs: number | null = null;
  let firstWaitingLabel: string | null = null;
  let firstStaleLabel: string | null = null;

  for (const desc of descriptors) {
    switch (desc.tone) {
      case 'attention':
        waitingCount += 1;
        if (firstWaitingLabel === null) {
          firstWaitingLabel = desc.label;
        }
        break;
      case 'working':
        workingCount += 1;
        break;
      case 'idle':
        idleCount += 1;
        break;
      case 'stale':
        staleCount += 1;
        if (firstStaleLabel === null) {
          firstStaleLabel = desc.label;
        }
        if (
          desc.sampleAgeMs !== null &&
          (oldestStaleAgeMs === null || desc.sampleAgeMs > oldestStaleAgeMs)
        ) {
          oldestStaleAgeMs = desc.sampleAgeMs;
        }
        break;
      case 'disconnected':
        disconnectedCount += 1;
        break;
      case 'unavailable':
        unavailableCount += 1;
        break;
    }
  }

  let primaryTone: TrayCoworkerTone;
  let primaryLabel: string;
  if (waitingCount > 0) {
    primaryTone = 'attention';
    primaryLabel =
      waitingCount === 1 && firstWaitingLabel !== null
        ? firstWaitingLabel
        : `${waitingCount} coworkers waiting`;
  } else if (workingCount > 0) {
    primaryTone = 'working';
    primaryLabel =
      workingCount === 1
        ? 'Coworker working'
        : `${workingCount} coworkers working`;
  } else if (staleCount > 0) {
    primaryTone = 'stale';
    primaryLabel =
      staleCount === 1 && firstStaleLabel !== null
        ? firstStaleLabel
        : `${staleCount} chats stale`;
  } else if (disconnectedCount > 0) {
    primaryTone = 'disconnected';
    primaryLabel =
      disconnectedCount === 1
        ? 'Coworker disconnected'
        : `${disconnectedCount} chats disconnected`;
  } else if (idleCount > 0) {
    primaryTone = 'idle';
    primaryLabel =
      idleCount === 1 ? 'Coworker idle' : `${idleCount} coworkers idle`;
  } else {
    primaryTone = 'unavailable';
    primaryLabel = 'Coworker unavailable';
  }

  const suffixes: string[] = [];
  if (primaryTone !== 'stale' && staleCount > 0) {
    suffixes.push(`${staleCount} stale`);
  }
  if (primaryTone !== 'disconnected' && disconnectedCount > 0) {
    suffixes.push(`${disconnectedCount} disconnected`);
  }
  const label =
    suffixes.length > 0
      ? `${primaryLabel} · ${suffixes.join(' · ')}`
      : primaryLabel;

  return {
    tone: primaryTone,
    label,
    workingCount,
    waitingCount,
    idleCount,
    staleCount,
    disconnectedCount,
    unavailableCount,
    oldestStaleAgeMs,
  };
}

function buildSourceFailureNotice(
  unavailableSources: ReadonlyArray<{
    sourceLabel: string;
    rawMessage: string | null;
  }>
): string | null {
  if (unavailableSources.length === 0) {
    return null;
  }
  if (unavailableSources.length === 1) {
    const first = unavailableSources[0]!;
    const raw = first.rawMessage
      ? `Source "${first.sourceLabel}" unavailable: ${first.rawMessage}`
      : `Source "${first.sourceLabel}" is unavailable.`;
    return truncateCodePoints(raw, WORKSPACE_TRAY_MAX_DETAIL_CHARS);
  }
  return truncateCodePoints(
    `${unavailableSources.length} observation sources are unavailable.`,
    WORKSPACE_TRAY_MAX_DETAIL_CHARS
  );
}

function resolveBlockerDetail(
  reason: string,
  linkedJob: WorkspaceJobView | null,
  fallbackMessageSummary: string | null
): string {
  const cleanReason = sanitizeWorkspaceSummary(reason);
  const concreteSummary =
    trimOrNull(linkedJob?.summary) ?? trimOrNull(fallbackMessageSummary);
  const genericReason =
    /reason not provided|no specific cause|requires operator attention|waiting for user input in the native chat|waiting for human approval in the native chat/i.test(
      cleanReason
    );
  if (concreteSummary !== null && genericReason) {
    return concreteSummary;
  }
  if (
    concreteSummary !== null &&
    cleanReason.length > 0 &&
    !cleanReason.toLowerCase().includes(concreteSummary.toLowerCase())
  ) {
    return `${cleanReason} · ${concreteSummary}`;
  }
  return cleanReason || concreteSummary || 'Action required in Codex.';
}

function resolveActiveJobDetail(job: WorkspaceJobView): string {
  const summary = trimOrNull(job.summary);
  if (summary !== null) {
    return summary;
  }
  const activeTask = job.tasks.find(
    (task) =>
      isActiveWorkerStatusToken(task.statusRaw) ||
      (task.currentAttempt !== null &&
        isActiveWorkerStatusToken(task.currentAttempt.statusRaw))
  );
  if (activeTask) {
    const attemptSummary = trimOrNull(activeTask.currentAttempt?.summary);
    return attemptSummary ?? activeTask.title;
  }
  if (job.tasks.length > 0) {
    return job.tasks[0]!.title;
  }
  return job.stateRaw === 'reviewing'
    ? 'Awaiting closure or task review.'
    : 'Job execution in progress.';
}

function formatWorkerCallDetail(
  summary: string | null,
  executorLabel: string,
  modelLabel: string | null
): string {
  const cleanSummary = trimOrNull(summary);
  if (cleanSummary !== null) {
    return cleanSummary;
  }
  const parts: string[] = [];
  if (
    executorLabel.length > 0 &&
    executorLabel !== 'Executor not recorded' &&
    executorLabel !== 'Executor recorded'
  ) {
    parts.push(executorLabel);
  }
  if (modelLabel !== null && modelLabel.length > 0) {
    parts.push(modelLabel);
  }
  return parts.join(' · ') || 'Worker executing';
}

function makeUnavailableProjection(
  unavailableReason: string,
  unavailableSourceCount: number = 0
): TrayProjection {
  return {
    status: 'unavailable',
    headerStatusLabel: 'Status unavailable',
    workingCount: null,
    attentionCount: null,
    pendingAcceptanceCount: 0,
    unavailableSourceCount,
    isStale: false,
    refreshError: null,
    sourceNotice: null,
    coworkerSummary: null,
    rows: [],
    emptyMessage: null,
    unavailableReason,
  };
}

interface CandidateRowMeta {
  tone: TrayRowTone;
  projectLabel: string;
  chatLabel: string;
  statusLabel: string;
  coworkerLabel: string | null;
  coworkerTone: TrayCoworkerTone | null;
  ageLabel: string;
}

interface RawTrayCandidate {
  sortPriority: number;
  updatedAtMs: number | null;
  tieKey: string;
  kind: TrayRowCategory;
  title: string;
  detail: string;
  chatIdentity: WorkspaceChatIdentity;
  jobId: string | null;
  meta: CandidateRowMeta;
}

function sortRawCandidates(candidates: RawTrayCandidate[]): RawTrayCandidate[] {
  return [...candidates].sort((left, right) => {
    if (left.sortPriority !== right.sortPriority) {
      return left.sortPriority - right.sortPriority;
    }
    const leftMs = left.updatedAtMs ?? Number.MIN_SAFE_INTEGER;
    const rightMs = right.updatedAtMs ?? Number.MIN_SAFE_INTEGER;
    if (rightMs !== leftMs) {
      return rightMs - leftMs;
    }
    return compareText(left.tieKey, right.tieKey);
  });
}

/**
 * Projects a typed `WorkspaceSnapshot` into the compact 3-row tray model.
 *
 * Invariants:
 * - Missing snapshots, zero configured sources, all-unavailable sources, or refresh
 *   errors with zero retained chats yield `status: 'unavailable'` with `null` counts.
 * - Retained snapshots on refresh failure preserve producer sample timestamps
 *   (`observed_at_ms` / `last_success_at_ms`) so sample age advances truthfully.
 * - Live native Coworker state (`working`, `idle`, `waiting_*`, `disconnected`, `stale`)
 *   is projected independently of Worker/job execution; Coworker `configuredSelection`
 *   is never used as runtime actor telemetry, and native `idle` never marks a job Done.
 * - Historical failures do not become `needs_human` blockers; completed Worker runs or
 *   unverified job completions are never shown as `Accepted` / `Done`.
 * - `boundWorkspaceCompactTrayRows` owns the 3-row cap, 1-terminal-result cap,
 *   `needs_human -> working -> result` priority order, and Unicode length bounds.
 */
export function projectTrayModel(
  snapshot: WorkspaceSnapshot | null,
  options: TrayProjectionOptions = {}
): TrayProjection {
  const rawError = trimOrNull(options.error);
  const errorText =
    rawError !== null ? normalizeTrayErrorMessage(rawError) : null;

  if (snapshot === null) {
    if (options.loading && errorText === null) {
      return {
        status: 'loading',
        headerStatusLabel: 'Loading status…',
        workingCount: null,
        attentionCount: null,
        pendingAcceptanceCount: 0,
        unavailableSourceCount: 0,
        isStale: false,
        refreshError: null,
        sourceNotice: null,
        coworkerSummary: null,
        rows: [],
        emptyMessage: null,
        unavailableReason: null,
      };
    }
    return makeUnavailableProjection(
      errorText ?? 'Workspace snapshot is not available.'
    );
  }

  const normalizedSnapshot = normalizeWorkspaceSnapshot(snapshot);
  const workspace = projectWorkspaceSnapshot(normalizedSnapshot, {
    nowMs: options.nowMs,
    loading: options.loading,
    error: errorText,
    archivePreferences: options.archivePreferences,
    preferenceStorage: options.preferenceStorage,
  });

  const sources = normalizedSnapshot.observation.sources;
  const unavailableSourceCount = workspace.unavailableSourceCount;
  const availableSourceCount = sources.filter(
    (src) => src.availability === 'available'
  ).length;

  const activeChats = workspace.activeProjects.flatMap((group) => group.chats);
  const archivedChats = workspace.archivedProjects.flatMap(
    (group) => group.chats
  );
  const totalChatCount = activeChats.length + archivedChats.length;

  if (
    workspace.status === 'unavailable' ||
    sources.length === 0 ||
    availableSourceCount === 0 ||
    (errorText !== null && totalChatCount === 0)
  ) {
    const firstUnavailableMsg = trimOrNull(
      workspace.unavailableSources[0]?.rawMessage
    );
    return makeUnavailableProjection(
      errorText ??
        firstUnavailableMsg ??
        workspace.unavailableReason ??
        'No reachable workspace observation source is currently available.',
      unavailableSourceCount
    );
  }

  const referenceNowMs = workspace.referenceNowMs;
  const sourceNotice = buildSourceFailureNotice(workspace.unavailableSources);

  const obsJobsByCompositeKey = new Map<string, ObservationJob>();
  for (const obsJob of normalizedSnapshot.observation.jobs) {
    obsJobsByCompositeKey.set(`${obsJob.source_id}\u0000${obsJob.id}`, obsJob);
  }

  const nativeSampleChatKeys = new Set<string>();
  for (const sample of normalizedSnapshot.native_samples) {
    nativeSampleChatKeys.add(
      `${sample.chat_identity.source_id}\u0000${sample.chat_identity.binding_id}\u0000${sample.chat_identity.thread_uuid}\u0000${sample.chat_identity.canonical_project_scope}`
    );
  }

  const resolveSelectedChat = (
    chat: WorkspaceChatSidebarItem
  ): WorkspaceSelectedChatProjection | null => {
    if (workspace.selectedChat?.chatKey === chat.chatKey) {
      return workspace.selectedChat;
    }
    return projectWorkspaceSnapshot(normalizedSnapshot, {
      nowMs: referenceNowMs,
      selectedChatKey: chat.chatKey,
      archivePreferences: options.archivePreferences,
      preferenceStorage: options.preferenceStorage,
    }).selectedChat;
  };

  let workingCount = 0;
  let attentionCount = 0;
  let pendingAcceptanceCount = 0;

  const coworkerDescriptors: ChatCoworkerDescriptor[] = [];
  const needsHumanCandidates: RawTrayCandidate[] = [];
  const workingCandidates: RawTrayCandidate[] = [];
  const realTerminalResultCandidates: RawTrayCandidate[] = [];
  const unacceptedCompletionCandidates: RawTrayCandidate[] = [];

  const candidateChats: WorkspaceChatSidebarItem[] = [
    ...activeChats,
    ...archivedChats.filter(
      (chat) =>
        chat.hasCurrentBlocker ||
        chat.workerSummary.hasActiveWorkers ||
        chat.jobSummary.activeJobCount > 0 ||
        (chat.nativeState.availability === 'available' &&
          chat.nativeState.activity === 'working')
    ),
  ];

  for (const chat of candidateChats) {
    const includeTerminalResults = chat.lifecycle === 'active';
    const rawIdentityKey = `${chat.identity.source_id}\u0000${chat.identity.binding_id}\u0000${chat.identity.thread_uuid}\u0000${chat.identity.canonical_project_scope}`;
    const hasExplicitNativeRecord =
      nativeSampleChatKeys.has(rawIdentityKey) ||
      chat.nativeState.observedAtMs !== null ||
      chat.nativeState.freshness === 'never_succeeded' ||
      chat.nativeState.connectionRoute !== null ||
      chat.nativeState.connectionDiagnostic !== null;

    const coworkerDesc = describeChatCoworkerState(
      chat,
      hasExplicitNativeRecord
    );
    if (coworkerDesc !== null && chat.lifecycle === 'active') {
      coworkerDescriptors.push(coworkerDesc);
    }

    const detailed = resolveSelectedChat(chat);
    const chatJobs = detailed?.jobs.items ?? [];
    const chatActivity = detailed?.activity.items ?? [];
    const chatMessages = detailed?.messages.items ?? [];

    const jobsById = new Map<string, WorkspaceJobView>();
    for (const job of chatJobs) {
      jobsById.set(job.jobId, job);
    }

    const latestQuestionMessage =
      chatMessages.find(
        (m) =>
          m.sourceKind === 'question_prompt' ||
          m.sourceKind === 'attention_summary'
      )?.summary ?? null;
    const latestProgressMessage =
      chatMessages.find(
        (m) =>
          m.sourceKind !== 'question_prompt' &&
          m.sourceKind !== 'attention_summary'
      )?.summary ?? null;

    // 1. Current human blockers (`needs_human`). Historical failures are never promoted here.
    const blockedJobIds = new Set<string>();
    for (const blocker of chat.blockers) {
      attentionCount += 1;
      if (blocker.jobId !== null) {
        blockedJobIds.add(blocker.jobId);
      }
      const linkedJob =
        blocker.jobId !== null ? (jobsById.get(blocker.jobId) ?? null) : null;
      const isNativeBlocker = blocker.origin === 'native_chat';
      const updatedAtMs = blocker.updatedAtMs ?? chat.updatedAtMs;
      const tone: TrayRowTone =
        blocker.kind === 'unresolved_effect' ? 'unknown' : 'attention';
      const statusLabel = formatBlockerStatusLabel(
        blocker.kind,
        blocker.origin,
        chat.nativeState.activity
      );
      const title = isNativeBlocker
        ? chat.threadLabel
        : (linkedJob?.title ?? chat.threadLabel);
      const detail = resolveBlockerDetail(
        blocker.reason,
        linkedJob,
        isNativeBlocker ? latestQuestionMessage : null
      );

      needsHumanCandidates.push({
        sortPriority: ATTENTION_KIND_PRIORITY[blocker.kind],
        updatedAtMs,
        tieKey: `${chat.chatKey}\u0000blocker\u0000${blocker.key}`,
        kind: 'needs_human',
        title,
        detail,
        chatIdentity: chat.identity,
        jobId: blocker.jobId,
        meta: {
          tone,
          projectLabel: chat.projectLabel,
          chatLabel: chat.threadLabel,
          statusLabel,
          coworkerLabel: isNativeBlocker ? null : (coworkerDesc?.label ?? null),
          coworkerTone: isNativeBlocker ? null : (coworkerDesc?.tone ?? null),
          ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
        },
      });
    }

    // Surface current `unknown` job or current-attempt states for operator verification,
    // never relabeling unknown outcomes as Done or Completed.
    for (const job of chatJobs) {
      if (blockedJobIds.has(job.jobId)) {
        continue;
      }
      const hasCurrentUnknownAttempt = job.tasks.some(
        (task) =>
          !task.isVerifiedAccepted &&
          (task.statusRaw === 'unknown' ||
            task.currentAttempt?.statusRaw === 'unknown')
      );
      if (job.stateRaw !== 'unknown' && !hasCurrentUnknownAttempt) {
        continue;
      }
      blockedJobIds.add(job.jobId);
      attentionCount += 1;
      const updatedAtMs = job.updatedAtMs ?? chat.updatedAtMs;
      needsHumanCandidates.push({
        sortPriority: ATTENTION_KIND_PRIORITY.unresolved_effect,
        updatedAtMs,
        tieKey: `${chat.chatKey}\u0000unknown\u0000${job.jobId}`,
        kind: 'needs_human',
        title: job.title,
        detail:
          trimOrNull(job.summary) ??
          'Execution stopped with unknown status; verify outcome before continuing.',
        chatIdentity: chat.identity,
        jobId: job.jobId,
        meta: {
          tone: 'unknown',
          projectLabel: chat.projectLabel,
          chatLabel: chat.threadLabel,
          statusLabel: 'Status unknown',
          coworkerLabel: coworkerDesc?.label ?? null,
          coworkerTone: coworkerDesc?.tone ?? null,
          ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
        },
      });
    }

    // 2. Active work (`working`), cleanly separating live Coworker activity from Worker/job runs.
    if (
      chat.nativeState.availability === 'available' &&
      chat.nativeState.activity === 'working'
    ) {
      workingCount += 1;
      const updatedAtMs = chat.nativeState.observedAtMs ?? chat.updatedAtMs;
      workingCandidates.push({
        sortPriority: 0,
        updatedAtMs,
        tieKey: `${chat.chatKey}\u0000coworker_working`,
        kind: 'working',
        title: chat.threadLabel,
        detail: latestProgressMessage ?? 'Coworker active in Codex chat.',
        chatIdentity: chat.identity,
        jobId: null,
        meta: {
          tone: 'working',
          projectLabel: chat.projectLabel,
          chatLabel: chat.threadLabel,
          statusLabel: 'Coworker working',
          coworkerLabel: null,
          coworkerTone: 'working',
          ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
        },
      });
    }

    interface RunningCallDescriptor {
      tieKey: string;
      title: string;
      detail: string;
      updatedAtMs: number | null;
      ageLabel: string;
      jobId: string | null;
    }

    const runningCallsByJobId = new Map<string, RunningCallDescriptor[]>();
    const unlinkedRunningCalls: RunningCallDescriptor[] = [];

    for (const act of chatActivity) {
      if (act.isLocalMcpCall || !act.running) {
        continue;
      }
      const runningChildren = act.children.filter(
        (child) => !child.isLocalMcpCall && child.running
      );
      if (runningChildren.length > 0) {
        for (const child of runningChildren) {
          const updatedAtMs = child.call.started_at_ms ?? act.updatedAtMs;
          const entry: RunningCallDescriptor = {
            tieKey: `${chat.chatKey}\u0000call\u0000${act.requestId}\u0000${child.callId}`,
            title: child.title,
            detail: formatWorkerCallDetail(
              child.summary,
              child.executorLabel,
              null
            ),
            updatedAtMs,
            ageLabel:
              child.durationLabel || formatAgeLabel(updatedAtMs, referenceNowMs),
            jobId: act.jobId,
          };
          if (act.jobId !== null) {
            const list = runningCallsByJobId.get(act.jobId) ?? [];
            list.push(entry);
            runningCallsByJobId.set(act.jobId, list);
          } else {
            unlinkedRunningCalls.push(entry);
          }
        }
      } else {
        const updatedAtMs = act.startedAtMs ?? act.updatedAtMs;
        const entry: RunningCallDescriptor = {
          tieKey: `${chat.chatKey}\u0000req\u0000${act.requestId}`,
          title: act.title,
          detail: formatWorkerCallDetail(
            act.summary,
            act.executorLabel,
            act.executorModelLabel
          ),
          updatedAtMs,
          ageLabel:
            act.durationLabel || formatAgeLabel(updatedAtMs, referenceNowMs),
          jobId: act.jobId,
        };
        if (act.jobId !== null) {
          const list = runningCallsByJobId.get(act.jobId) ?? [];
          list.push(entry);
          runningCallsByJobId.set(act.jobId, list);
        } else {
          unlinkedRunningCalls.push(entry);
        }
      }
    }

    for (const callEntry of unlinkedRunningCalls) {
      workingCount += 1;
      workingCandidates.push({
        sortPriority: 0,
        updatedAtMs: callEntry.updatedAtMs,
        tieKey: callEntry.tieKey,
        kind: 'working',
        title: callEntry.title,
        detail: callEntry.detail,
        chatIdentity: chat.identity,
        jobId: null,
        meta: {
          tone: 'working',
          projectLabel: chat.projectLabel,
          chatLabel: chat.threadLabel,
          statusLabel: 'Worker running',
          coworkerLabel: coworkerDesc?.label ?? null,
          coworkerTone: coworkerDesc?.tone ?? null,
          ageLabel: callEntry.ageLabel,
        },
      });
    }

    const handledRunningJobIds = new Set<string>();
    for (const job of chatJobs) {
      const activeRunCount = job.runs.filter((run) =>
        isActiveWorkerStatusToken(run.status)
      ).length;
      const activeAttemptCount = job.tasks.filter(
        (task) =>
          task.currentAttempt !== null &&
          isActiveWorkerStatusToken(task.currentAttempt.statusRaw)
      ).length;
      const linkedCalls = runningCallsByJobId.get(job.jobId) ?? [];
      handledRunningJobIds.add(job.jobId);

      const obsActiveCalls =
        obsJobsByCompositeKey.get(`${job.sourceId}\u0000${job.jobId}`)
          ?.active_call_count ?? 0;
      const workerUnits = Math.max(
        activeRunCount,
        activeAttemptCount,
        linkedCalls.length,
        obsActiveCalls > 0 ? obsActiveCalls : 0
      );

      const isJobActivelyProgressing =
        !blockedJobIds.has(job.jobId) &&
        (job.stateRaw === 'running' ||
          job.stateRaw === 'reviewing' ||
          job.stateRaw === 'planning');

      if (workerUnits === 0 && !isJobActivelyProgressing) {
        continue;
      }

      const countedUnits = workerUnits > 0 ? workerUnits : 1;
      workingCount += countedUnits;

      if (linkedCalls.length > 0) {
        for (const callEntry of linkedCalls) {
          workingCandidates.push({
            sortPriority: 0,
            updatedAtMs: callEntry.updatedAtMs,
            tieKey: callEntry.tieKey,
            kind: 'working',
            title: callEntry.title,
            detail: callEntry.detail,
            chatIdentity: chat.identity,
            jobId: job.jobId,
            meta: {
              tone: 'working',
              projectLabel: chat.projectLabel,
              chatLabel: chat.threadLabel,
              statusLabel: 'Worker running',
              coworkerLabel: coworkerDesc?.label ?? null,
              coworkerTone: coworkerDesc?.tone ?? null,
              ageLabel: callEntry.ageLabel,
            },
          });
        }
        continue;
      }

      const updatedAtMs = job.updatedAtMs ?? chat.updatedAtMs;
      let statusLabel: string;
      if (job.stateRaw === 'reviewing') {
        statusLabel = 'Reviewing';
      } else if (job.stateRaw === 'planning') {
        statusLabel = 'Planning';
      } else if (workerUnits > 1) {
        statusLabel = `Workers running (${workerUnits})`;
      } else if (workerUnits === 1) {
        statusLabel = 'Worker running';
      } else {
        statusLabel = 'Running';
      }

      workingCandidates.push({
        sortPriority: 0,
        updatedAtMs,
        tieKey: `${chat.chatKey}\u0000job_work\u0000${job.jobId}`,
        kind: 'working',
        title: job.title,
        detail: resolveActiveJobDetail(job),
        chatIdentity: chat.identity,
        jobId: job.jobId,
        meta: {
          tone: 'working',
          projectLabel: chat.projectLabel,
          chatLabel: chat.threadLabel,
          statusLabel,
          coworkerLabel: coworkerDesc?.label ?? null,
          coworkerTone: coworkerDesc?.tone ?? null,
          ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
        },
      });
    }

    for (const [jobId, linkedCalls] of runningCallsByJobId.entries()) {
      if (handledRunningJobIds.has(jobId)) {
        continue;
      }
      for (const callEntry of linkedCalls) {
        workingCount += 1;
        workingCandidates.push({
          sortPriority: 0,
          updatedAtMs: callEntry.updatedAtMs,
          tieKey: callEntry.tieKey,
          kind: 'working',
          title: callEntry.title,
          detail: callEntry.detail,
          chatIdentity: chat.identity,
          jobId,
          meta: {
            tone: 'working',
            projectLabel: chat.projectLabel,
            chatLabel: chat.threadLabel,
            statusLabel: 'Worker running',
            coworkerLabel: coworkerDesc?.label ?? null,
            coworkerTone: coworkerDesc?.tone ?? null,
            ageLabel: callEntry.ageLabel,
          },
        });
      }
    }

    if (!includeTerminalResults) {
      continue;
    }

    // 3. Terminal outcomes vs unaccepted Worker/job outputs.
    // Completed Worker runs or unverified job completions are never shown as Accepted or Done.
    let accountedCompletedWorkerOnJob = false;
    for (const job of chatJobs) {
      if (
        blockedJobIds.has(job.jobId) ||
        job.stateRaw === 'running' ||
        job.stateRaw === 'reviewing' ||
        job.stateRaw === 'planning'
      ) {
        continue;
      }

      const updatedAtMs =
        job.result?.producedAtMs ?? job.updatedAtMs ?? chat.updatedAtMs;

      if (job.isVerifiedAccepted) {
        accountedCompletedWorkerOnJob = true;
        realTerminalResultCandidates.push({
          sortPriority: 0,
          updatedAtMs,
          tieKey: `${chat.chatKey}\u0000res_accepted\u0000${job.jobId}`,
          kind: 'result',
          title: job.title,
          detail:
            trimOrNull(job.result?.summary) ??
            trimOrNull(job.summary) ??
            'Verified accepted job completion.',
          chatIdentity: chat.identity,
          jobId: job.jobId,
          meta: {
            tone: 'success',
            projectLabel: chat.projectLabel,
            chatLabel: chat.threadLabel,
            statusLabel: 'Verified accepted',
            coworkerLabel: coworkerDesc?.label ?? null,
            coworkerTone: coworkerDesc?.tone ?? null,
            ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
          },
        });
        continue;
      }

      if (
        job.stateRaw === 'failed' ||
        job.acceptanceProof.status === 'rejected'
      ) {
        accountedCompletedWorkerOnJob = true;
        const isRejected =
          job.stateRaw !== 'failed' &&
          job.acceptanceProof.status === 'rejected';
        realTerminalResultCandidates.push({
          sortPriority: 0,
          updatedAtMs,
          tieKey: `${chat.chatKey}\u0000res_failed\u0000${job.jobId}`,
          kind: 'result',
          title: job.title,
          detail:
            trimOrNull(job.result?.summary) ??
            trimOrNull(job.summary) ??
            (isRejected ? 'Closure review rejected candidate.' : 'Job failed.'),
          chatIdentity: chat.identity,
          jobId: job.jobId,
          meta: {
            tone: 'danger',
            projectLabel: chat.projectLabel,
            chatLabel: chat.threadLabel,
            statusLabel: isRejected ? 'Closure rejected' : 'Failed',
            coworkerLabel: coworkerDesc?.label ?? null,
            coworkerTone: coworkerDesc?.tone ?? null,
            ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
          },
        });
        continue;
      }

      const hasUnacceptedWorkerOutput =
        job.stateRaw === 'completed' ||
        job.acceptanceProof.status === 'pending_review' ||
        job.acceptanceProof.status === 'not_accepted' ||
        job.tasks.some(
          (t) =>
            !t.isVerifiedAccepted &&
            (t.candidates.length > 0 ||
              t.statusRaw === 'completed' ||
              t.currentAttempt?.statusRaw === 'completed')
        ) ||
        job.runs.some((r) => {
          const lower = r.status.trim().toLowerCase();
          return lower === 'completed' || lower === 'succeeded' || lower === 'done';
        });

      if (hasUnacceptedWorkerOutput) {
        accountedCompletedWorkerOnJob = true;
        pendingAcceptanceCount += 1;
        const statusLabel =
          job.acceptanceProof.status === 'not_accepted'
            ? 'Not accepted'
            : job.acceptanceProof.status === 'pending_review'
              ? 'Pending review'
              : 'Awaiting acceptance';
        unacceptedCompletionCandidates.push({
          sortPriority: 1,
          updatedAtMs,
          tieKey: `${chat.chatKey}\u0000unaccepted\u0000${job.jobId}`,
          kind: 'result',
          title: job.title,
          detail:
            trimOrNull(job.result?.summary) ??
            trimOrNull(job.summary) ??
            'Worker output recorded; awaiting verified acceptance.',
          chatIdentity: chat.identity,
          jobId: job.jobId,
          meta: {
            tone: 'neutral',
            projectLabel: chat.projectLabel,
            chatLabel: chat.threadLabel,
            statusLabel,
            coworkerLabel: coworkerDesc?.label ?? null,
            coworkerTone: coworkerDesc?.tone ?? null,
            ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
          },
        });
      }
    }

    if (
      !accountedCompletedWorkerOnJob &&
      chat.workerSummary.completedWorkerRunCount > 0
    ) {
      pendingAcceptanceCount += 1;
    }

    for (const act of chatActivity) {
      if (act.isLocalMcpCall || act.running || act.jobId !== null) {
        continue;
      }
      if (act.stateRaw !== 'failed' && act.stateRaw !== 'cancelled') {
        continue;
      }
      const isFailed = act.stateRaw === 'failed';
      const updatedAtMs = act.finishedAtMs ?? act.updatedAtMs;
      realTerminalResultCandidates.push({
        sortPriority: 0,
        updatedAtMs,
        tieKey: `${chat.chatKey}\u0000act_res\u0000${act.requestId}`,
        kind: 'result',
        title: act.title,
        detail:
          trimOrNull(act.summary) ??
          (isFailed ? 'Worker execution failed.' : 'Worker execution cancelled.'),
        chatIdentity: chat.identity,
        jobId: null,
        meta: {
          tone: isFailed ? 'danger' : 'neutral',
          projectLabel: chat.projectLabel,
          chatLabel: chat.threadLabel,
          statusLabel: isFailed ? 'Worker failed' : 'Cancelled',
          coworkerLabel: coworkerDesc?.label ?? null,
          coworkerTone: coworkerDesc?.tone ?? null,
          ageLabel: formatAgeLabel(updatedAtMs, referenceNowMs),
        },
      });
    }
  }

  // Real terminal results always take precedence over unaccepted Worker/job outputs;
  // `boundWorkspaceCompactTrayRows` enforces the 3-row cap, 1-result cap, priority order,
  // and Unicode code-point limits.
  const selectedResultPool =
    realTerminalResultCandidates.length > 0
      ? realTerminalResultCandidates
      : unacceptedCompletionCandidates;

  const orderedCandidates = [
    ...sortRawCandidates(needsHumanCandidates),
    ...sortRawCandidates(workingCandidates),
    ...sortRawCandidates(selectedResultPool),
  ];

  const metaBySafeId = new Map<string, CandidateRowMeta>();
  const compactInputs: WorkspaceCompactTrayRow[] = orderedCandidates.map(
    (candidate, index) => {
      const safeId = `tray.${candidate.kind}.${String(index + 1).padStart(4, '0')}`;
      metaBySafeId.set(safeId, candidate.meta);
      return {
        id: safeId,
        kind: candidate.kind,
        title: candidate.title,
        detail: candidate.detail,
        updated_at_ms: candidate.updatedAtMs,
        chat_identity: candidate.chatIdentity,
        job_id: candidate.jobId,
      };
    }
  );

  const boundedCompactRows = boundWorkspaceCompactTrayRows(compactInputs);
  const rows: TrayRowView[] = boundedCompactRows.map((row) => {
    const meta = metaBySafeId.get(row.id) ?? {
      tone: 'neutral' as const,
      projectLabel: 'Project',
      chatLabel: 'Codex chat',
      statusLabel: 'Recorded',
      coworkerLabel: null,
      coworkerTone: null,
      ageLabel: formatAgeLabel(row.updated_at_ms, referenceNowMs),
    };
    const codexTarget: CodexChatTarget | null =
      row.chat_identity !== null
        ? validateTrayCodexChatTarget({
            source_id: row.chat_identity.source_id,
            thread_uuid: row.chat_identity.thread_uuid,
            job_id: row.job_id,
          })
        : null;

    return {
      key: row.id,
      category: row.kind,
      tone: meta.tone,
      title: row.title,
      projectLabel: meta.projectLabel,
      chatLabel: meta.chatLabel,
      statusLabel: meta.statusLabel,
      coworkerLabel: meta.coworkerLabel,
      coworkerTone: meta.coworkerTone,
      detail: row.detail,
      updatedAtMs: row.updated_at_ms,
      ageLabel: meta.ageLabel,
      chatIdentity: row.chat_identity,
      threadUuid: row.chat_identity?.thread_uuid ?? null,
      jobId: row.job_id,
      codexTarget,
    };
  });

  const coworkerSummary = buildAggregateCoworkerSummary(coworkerDescriptors);
  const headerStatusLabel = formatTrayHeaderSummary(
    workingCount,
    attentionCount
  );
  const isStale = workspace.isStale || coworkerSummary?.tone === 'stale';

  if (rows.length === 0 && workingCount === 0 && attentionCount === 0) {
    let emptyMessage: string;
    if (pendingAcceptanceCount > 0) {
      emptyMessage =
        coworkerSummary !== null
          ? `${coworkerSummary.label} · worker output awaiting verified acceptance.`
          : 'Worker output recorded; awaiting verified job acceptance.';
    } else if (coworkerSummary !== null) {
      if (coworkerSummary.tone === 'stale') {
        emptyMessage = `${coworkerSummary.label} · live chat activity withheld until a fresh sample arrives.`;
      } else if (coworkerSummary.tone === 'disconnected') {
        emptyMessage = `${coworkerSummary.label} · live chat activity unavailable.`;
      } else {
        emptyMessage = `${coworkerSummary.label} · no active or recent jobs.`;
      }
    } else {
      emptyMessage = 'No active or recent work.';
    }

    return {
      status: 'empty',
      headerStatusLabel,
      workingCount: 0,
      attentionCount: 0,
      pendingAcceptanceCount,
      unavailableSourceCount,
      isStale,
      refreshError: errorText,
      sourceNotice,
      coworkerSummary,
      rows: [],
      emptyMessage,
      unavailableReason: null,
    };
  }

  return {
    status: 'ready',
    headerStatusLabel,
    workingCount,
    attentionCount,
    pendingAcceptanceCount,
    unavailableSourceCount,
    isStale,
    refreshError: errorText,
    sourceNotice,
    coworkerSummary,
    rows,
    emptyMessage: null,
    unavailableReason: null,
  };
}

export interface TrayCoordinatorState {
  snapshot: WorkspaceSnapshot | null;
  loading: boolean;
  error: string | null;
  openError: string | null;
  openingChat: boolean;
}

export interface TrayCoordinatorOptions {
  openCodexChat?: (target: CodexChatTarget) => Promise<void>;
  visible?: boolean;
  focused?: boolean;
}

/**
 * Coordinates tray `WorkspaceSnapshot` reads and narrow `CodexChatTarget` navigation.
 *
 * Guarantees:
 * - Opening triggers a single current snapshot read (coalescing simultaneous open/focus signals).
 * - Duplicate in-flight reads coalesce onto the active request.
 * - Forced refreshes and open/close/reopen transitions invalidate older sequence numbers
 *   and reject stale responses.
 * - Hidden or unfocused windows never poll.
 * - Fetch failures retain any previously loaded `WorkspaceSnapshot` without resetting
 *   producer `observed_at_ms` or `last_success_at_ms` timestamps.
 */
export class TrayObservationCoordinator {
  private readonly fetcher: (
    request?: ObservationSnapshotRequest
  ) => Promise<WorkspaceSnapshot>;
  private readonly onChange?: (state: TrayCoordinatorState) => void;
  private readonly openCodexChatPort?: (
    target: CodexChatTarget
  ) => Promise<void>;
  private state: TrayCoordinatorState;
  private inFlightPromise: Promise<void> | null = null;
  private queuedForceRefresh = false;
  private requestSeq = 0;
  private activeRequestSeq = 0;
  private latestIssuedSeq = 0;
  private visible: boolean;
  private focused: boolean;
  private openSessionReadStarted = false;
  private activeRequestIsOpenTransition = false;
  private disposed = false;

  public constructor(
    fetcher: (
      request?: ObservationSnapshotRequest
    ) => Promise<WorkspaceSnapshot>,
    onChange?: (state: TrayCoordinatorState) => void,
    options: TrayCoordinatorOptions = {}
  ) {
    this.fetcher = fetcher;
    this.onChange = onChange;
    this.openCodexChatPort = options.openCodexChat;
    this.visible = options.visible ?? true;
    this.focused = options.focused ?? true;
    this.state = {
      snapshot: null,
      loading: this.visible,
      error: null,
      openError: null,
      openingChat: false,
    };
  }

  public getState(): TrayCoordinatorState {
    return { ...this.state };
  }

  public isInFlight(): boolean {
    return this.inFlightPromise !== null;
  }

  public isDisposed(): boolean {
    return this.disposed;
  }

  public setVisibility(visible: boolean, focused: boolean): void {
    if (this.disposed) {
      return;
    }
    this.visible = visible;
    this.focused = focused;
    if (!visible) {
      this.queuedForceRefresh = false;
      this.openSessionReadStarted = false;
      this.activeRequestIsOpenTransition = false;
    }
  }

  /**
   * Synchronizes window visibility/focus transitions from DOM events without
   * triggering duplicate fetches when `open()` already initiated the open read.
   */
  public notifyVisibilityTransition(
    visible: boolean,
    focused: boolean
  ): Promise<void> {
    if (this.disposed) {
      return Promise.resolve();
    }
    const wasVisible = this.visible;
    this.setVisibility(visible, focused);
    if (!visible) {
      return Promise.resolve();
    }
    if (!wasVisible && !this.openSessionReadStarted) {
      this.openSessionReadStarted = true;
      return this.startOrCoalesceOpenRead(true);
    }
    return Promise.resolve();
  }

  /**
   * Handles explicit tray popover open (`threadlane:tray-opened` or initial visible mount)
   * with exactly one fresh read for the open transition.
   */
  public open(): Promise<void> {
    if (this.disposed) {
      return Promise.resolve();
    }
    this.visible = true;
    this.focused = true;
    this.openSessionReadStarted = true;
    if (
      this.inFlightPromise !== null &&
      this.activeRequestIsOpenTransition &&
      this.activeRequestSeq === this.latestIssuedSeq
    ) {
      return this.inFlightPromise;
    }
    return this.startOrCoalesceOpenRead(true);
  }

  /**
   * Marks the tray popover hidden/closed, cancelling queued polls and rejecting
   * any stale pre-close in-flight response.
   */
  public close(): void {
    if (this.disposed) {
      return;
    }
    this.visible = false;
    this.focused = false;
    this.queuedForceRefresh = false;
    this.openSessionReadStarted = false;
    this.activeRequestIsOpenTransition = false;
    this.latestIssuedSeq = ++this.requestSeq;
    if (this.state.loading) {
      this.state = {
        ...this.state,
        loading: false,
      };
      this.emitChange();
    }
  }

  public canPoll(): boolean {
    return !this.disposed && this.visible && this.focused;
  }

  /**
   * Triggers a `WorkspaceSnapshot` read bounded to `WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT` (50).
   * - Hidden windows ignore non-forced background refreshes.
   * - Duplicate non-forced reads coalesce onto an active valid in-flight read.
   * - `force: true` invalidates any older sequence and queues at most one follow-up read.
   */
  public refresh(options: { force?: boolean } = {}): Promise<void> {
    if (this.disposed) {
      return Promise.resolve();
    }
    if (!options.force && !this.visible) {
      return Promise.resolve();
    }
    return this.startOrCoalesceOpenRead(Boolean(options.force), false);
  }

  public async openCodexChat(target: CodexChatTarget): Promise<boolean> {
    if (this.disposed) {
      return false;
    }
    const validated = validateTrayCodexChatTarget(target);
    if (validated === null) {
      this.state = {
        ...this.state,
        openError:
          'Cannot open Codex chat: target requires a verified source and canonical thread UUID.',
      };
      this.emitChange();
      return false;
    }
    if (!this.openCodexChatPort) {
      this.state = {
        ...this.state,
        openError:
          'Opening a conversation in Codex is unavailable on this route.',
      };
      this.emitChange();
      return false;
    }
    if (this.state.openingChat) {
      return false;
    }

    this.state = {
      ...this.state,
      openingChat: true,
      openError: null,
    };
    this.emitChange();

    try {
      await this.openCodexChatPort(validated);
      if (!this.disposed) {
        this.state = {
          ...this.state,
          openingChat: false,
          openError: null,
        };
        this.emitChange();
      }
      return true;
    } catch (err) {
      if (!this.disposed) {
        this.state = {
          ...this.state,
          openingChat: false,
          openError: normalizeTrayErrorMessage(
            err,
            'Unable to open the selected conversation in Codex.'
          ),
        };
        this.emitChange();
      }
      return false;
    }
  }

  public clearOpenError(): void {
    if (!this.disposed && this.state.openError !== null) {
      this.state = {
        ...this.state,
        openError: null,
      };
      this.emitChange();
    }
  }

  public dispose(): void {
    this.disposed = true;
    this.visible = false;
    this.focused = false;
    this.queuedForceRefresh = false;
    this.openSessionReadStarted = false;
    this.activeRequestIsOpenTransition = false;
    this.latestIssuedSeq = ++this.requestSeq;
  }

  private startOrCoalesceOpenRead(
    force: boolean,
    isOpenTransition: boolean = force
  ): Promise<void> {
    if (this.inFlightPromise !== null) {
      const inFlightInvalidated = this.activeRequestSeq !== this.latestIssuedSeq;
      if (force || inFlightInvalidated) {
        this.queuedForceRefresh = true;
        this.activeRequestIsOpenTransition = isOpenTransition;
        this.latestIssuedSeq = ++this.requestSeq;
        if (!this.state.loading) {
          this.state = {
            ...this.state,
            loading: true,
            ...(this.state.snapshot === null ? { error: null } : {}),
          };
          this.emitChange();
        }
      }
      return this.inFlightPromise;
    }
    return this.startRequest(isOpenTransition);
  }

  private startRequest(isOpenTransition: boolean): Promise<void> {
    const seq = ++this.requestSeq;
    this.activeRequestSeq = seq;
    this.latestIssuedSeq = seq;
    this.activeRequestIsOpenTransition = isOpenTransition;

    if (
      !this.state.loading ||
      (this.state.snapshot === null && this.state.error !== null)
    ) {
      this.state = {
        ...this.state,
        loading: true,
        ...(this.state.snapshot === null ? { error: null } : {}),
      };
      this.emitChange();
    }

    const run = async (): Promise<void> => {
      try {
        const snapshot = await this.fetcher({
          limit: WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT,
        });
        if (this.disposed || seq !== this.latestIssuedSeq) {
          return;
        }
        this.state = {
          ...this.state,
          snapshot,
          loading: false,
          error: null,
        };
        this.emitChange();
      } catch (err) {
        if (this.disposed || seq !== this.latestIssuedSeq) {
          return;
        }
        // Retain any previously loaded WorkspaceSnapshot untouched so producer
        // `observed_at_ms` and `last_success_at_ms` timestamps are never reset.
        this.state = {
          ...this.state,
          loading: false,
          error: normalizeTrayErrorMessage(
            err,
            'Workspace status is unavailable.'
          ),
        };
        this.emitChange();
      } finally {
        this.inFlightPromise = null;
        if (!this.disposed && this.queuedForceRefresh) {
          const nextIsOpenTransition = this.activeRequestIsOpenTransition;
          this.queuedForceRefresh = false;
          this.activeRequestIsOpenTransition = false;
          await this.startRequest(nextIsOpenTransition);
        } else {
          this.activeRequestIsOpenTransition = false;
        }
      }
    };

    const promise = run();
    this.inFlightPromise = promise;
    return promise;
  }

  private emitChange(): void {
    if (!this.disposed) {
      this.onChange?.({ ...this.state });
    }
  }
}
