/**
 * Pure projection, preference normalization, and async request coordination
 * for the Threadlane system tray popover and window-close preference controls.
 *
 * This module has no DOM, CSS, SVG, or Tauri runtime imports so it can be
 * validated directly under `node --experimental-strip-types --test`.
 */
import type {
  ObservationCall,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
} from './observation/types.ts';
import {
  compactFeedText,
  formatRelativeTime,
  isLocalMcpExecutor,
  shouldShowSpinner,
} from './pages/activity-feed-model.ts';
import {
  projectJobLookupKey,
  projectProjects,
  projectRequestLookupKey,
  safeTimestampMs,
  type ProjectAttentionKind,
} from './pages/projects-model.ts';

export const TRAY_WINDOW_LABEL = 'tray' as const;
export const TRAY_WINDOW_URL = 'index.html?surface=tray' as const;
export const TRAY_OPEN_EVENT = 'threadlane:tray-opened' as const;
export const TRAY_PREFERENCES_EVENT = 'threadlane:tray-preferences-changed' as const;
export const TRAY_GET_PREFERENCES_COMMAND = 'desktop_tray_preferences' as const;
export const TRAY_SET_PREFERENCES_COMMAND = 'desktop_set_tray_preferences' as const;
export const TRAY_ACTION_COMMAND = 'desktop_tray_action' as const;
export const DEFAULT_TRAY_CLOSE_BEHAVIOR: TrayCloseBehavior = 'hide_to_tray';
export const TRAY_MAX_ROWS = 3;
export const TRAY_REFRESH_INTERVAL_MS = 4000;

export type TrayCloseBehavior = 'hide_to_tray' | 'quit';
export type TrayAction = 'show_main' | 'hide' | 'quit';

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
export type TrayRowCategory = 'needs_human' | 'working' | 'result';
export type TrayRowTone =
  | 'attention'
  | 'working'
  | 'success'
  | 'danger'
  | 'unknown'
  | 'neutral';

export interface TrayRowView {
  key: string;
  category: TrayRowCategory;
  tone: TrayRowTone;
  title: string;
  projectLabel: string;
  chatLabel: string;
  statusLabel: string;
  detail: string;
  updatedAtMs: number | null;
  ageLabel: string;
  threadUuid: string | null;
}

export interface TrayProjection {
  status: TrayStatusKind;
  headerStatusLabel: string;
  workingCount: number | null;
  attentionCount: number | null;
  rows: TrayRowView[];
  emptyMessage: string | null;
  unavailableReason: string | null;
}

export interface TrayProjectionOptions {
  loading?: boolean;
  error?: string | null;
  nowMs?: number;
}

const ATTENTION_PRIORITY: Record<ProjectAttentionKind, number> = {
  on_hold: 0,
  status_check: 1,
  plan_approval: 2,
  awaiting_review: 3,
  recorded_wait: 4,
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

export function isTrayCloseBehavior(value: unknown): value is TrayCloseBehavior {
  return value === 'hide_to_tray' || value === 'quit';
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
      : err && typeof err === 'object' && 'message' in err && typeof (err as { message: unknown }).message === 'string'
        ? (err as { message: string }).message
        : fallback;
  const cleaned = raw
    .replace(/bearer\s+[a-z0-9._-]+/gi, '[redacted]')
    .replace(/(api_?key|secret|token|password)\s*[=:]\s*[^\s,;]+/gi, '$1=[redacted]')
    .trim();
  if (!cleaned) {
    return fallback;
  }
  return cleaned.length > 200 ? `${cleaned.slice(0, 197)}…` : cleaned;
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

function formatAttentionStatusLabel(
  kind: ProjectAttentionKind,
  rawStateLabel: string
): string {
  switch (kind) {
    case 'on_hold':
      return 'On hold';
    case 'plan_approval':
      return 'Awaiting approval';
    case 'awaiting_review':
      return rawStateLabel === 'Changes requested'
        ? 'Changes requested'
        : 'Ready for review';
    case 'status_check':
      return 'Status needs checking';
    case 'recorded_wait':
      return 'Needs you';
  }
}

function resolveAttentionDetail(
  reason: string,
  linkedJob: ObservationJob | null,
  linkedRequest: ObservationRequest | null
): string {
  const jobSummary = compactFeedText(linkedJob?.summary, 140);
  const requestSummary = compactFeedText(linkedRequest?.summary, 140);
  const concreteSummary = jobSummary || requestSummary;
  const genericReason =
    /reason not provided|no specific cause was recorded/i.test(reason);
  if (concreteSummary && genericReason) {
    return concreteSummary;
  }
  if (
    concreteSummary &&
    !reason.toLowerCase().includes(concreteSummary.toLowerCase())
  ) {
    return compactFeedText(`${reason} · ${concreteSummary}`, 160);
  }
  return compactFeedText(reason, 160) || 'Action required in Codex.';
}

function formatAgeLabel(atMs: number | null, nowMs: number): string {
  if (atMs === null) {
    return 'Time not reported';
  }
  return formatRelativeTime(atMs, nowMs);
}

interface IndexedRequestCall {
  callId: string;
  sourceId: string;
  jobId: string | null;
  running: boolean;
}

function indexCallsFromSnapshot(
  snapshot: ObservationSnapshot
): Map<string, IndexedRequestCall> {
  const byCallKey = new Map<string, IndexedRequestCall>();
  for (const request of snapshot.requests ?? []) {
    const jobId = trimOrNull(request.job_id);
    const children = request.children ?? [];
    const agentChildren = children.filter(
      (child: ObservationCall) => !isLocalMcpExecutor(child.executor)
    );
    if (agentChildren.length > 0) {
      for (const child of agentChildren) {
        const key = JSON.stringify(['call', request.source_id, child.id]);
        byCallKey.set(key, {
          callId: child.id,
          sourceId: request.source_id,
          jobId,
          running: shouldShowSpinner(child.state),
        });
      }
      continue;
    }
    if (!isLocalMcpExecutor(request.executor)) {
      const key = JSON.stringify(['call', request.source_id, request.id]);
      byCallKey.set(key, {
        callId: request.id,
        sourceId: request.source_id,
        jobId,
        running: shouldShowSpinner(request.state),
      });
    }
  }
  return byCallKey;
}

/**
 * Projects a live `ObservationSnapshot` into the bounded 3-row tray model.
 *
 * Invariants:
 * - Missing, errored, or unreachable observation data yields `status: 'unavailable'`
 *   with `headerStatusLabel: 'Status unavailable'` and `null` counts (never healthy `0`).
 * - `workingCount` and `attentionCount` are tallied independently so concurrent
 *   running work and pending human decisions never mask each other.
 * - Rows are capped at `TRAY_MAX_ROWS` (3) and ordered strictly:
 *   1. `needs_human` (including `unknown` / `status_check`)
 *   2. `working`
 *   3. at most 1 most recent meaningful `result` (`Completed`, `Failed`, `Cancelled`).
 * - A `failed` or `unknown` state is never relabeled as `Completed`.
 */
export function projectTrayModel(
  snapshot: ObservationSnapshot | null,
  options: TrayProjectionOptions = {}
): TrayProjection {
  const errorText = trimOrNull(options.error);
  if (errorText !== null) {
    return {
      status: 'unavailable',
      headerStatusLabel: 'Status unavailable',
      workingCount: null,
      attentionCount: null,
      rows: [],
      emptyMessage: null,
      unavailableReason: errorText,
    };
  }

  if (snapshot === null) {
    if (options.loading) {
      return {
        status: 'loading',
        headerStatusLabel: 'Loading status…',
        workingCount: null,
        attentionCount: null,
        rows: [],
        emptyMessage: null,
        unavailableReason: null,
      };
    }
    return {
      status: 'unavailable',
      headerStatusLabel: 'Status unavailable',
      workingCount: null,
      attentionCount: null,
      rows: [],
      emptyMessage: null,
      unavailableReason: 'Observation snapshot is not available.',
    };
  }

  const nowMs =
    safeTimestampMs(options.nowMs) ??
    safeTimestampMs(snapshot.generated_at_ms) ??
    Date.now();

  const sources = snapshot.sources ?? [];
  const availableSources = sources.filter(
    (source) => source.availability === 'available'
  );
  const unavailableSources = sources.filter(
    (source) => source.availability !== 'available'
  );

  const projected = projectProjects(snapshot, {
    nowMs,
    windowHours: 24,
  });

  if (
    sources.length === 0 ||
    availableSources.length === 0 ||
    (projected.projects.length === 0 && unavailableSources.length > 0)
  ) {
    const firstUnavailableMsg = trimOrNull(unavailableSources[0]?.message);
    return {
      status: 'unavailable',
      headerStatusLabel: 'Status unavailable',
      workingCount: null,
      attentionCount: null,
      rows: [],
      emptyMessage: null,
      unavailableReason:
        firstUnavailableMsg ??
        'No reachable observation source is currently available.',
    };
  }

  const jobsByKey = new Map<string, ObservationJob>();
  for (const job of snapshot.jobs ?? []) {
    jobsByKey.set(projectJobLookupKey(job.source_id, job.id), job);
  }
  const requestsByKey = new Map<string, ObservationRequest>();
  for (const request of snapshot.requests ?? []) {
    requestsByKey.set(projectRequestLookupKey(request.source_id, request.id), request);
  }
  const indexedCalls = indexCallsFromSnapshot(snapshot);

  let workingCount = 0;
  let attentionCount = 0;

  interface NeedsHumanCandidate {
    priority: number;
    updatedAtMs: number | null;
    row: TrayRowView;
  }

  interface TimedCandidate {
    updatedAtMs: number | null;
    row: TrayRowView;
  }

  const needsHumanCandidates: NeedsHumanCandidate[] = [];
  const workingCandidates: TimedCandidate[] = [];
  const resultCandidates: TimedCandidate[] = [];

  for (const project of projected.projects) {
    for (const session of project.sessions) {
      const verifiedThreadUuid = trimOrNull(session.threadUuid);

      // 1. Needs-human / attention items (never masked by concurrent running work)
      for (const item of session.attentionItems) {
        attentionCount += 1;
        const linkedJob =
          item.jobId !== null
            ? jobsByKey.get(projectJobLookupKey(item.sourceId, item.jobId)) ??
              null
            : null;
        const linkedRequest =
          item.requestId !== null
            ? requestsByKey.get(
                projectRequestLookupKey(item.sourceId, item.requestId)
              ) ?? null
            : null;
        const updatedAtMs = item.updatedAtMs ?? session.updatedAtMs;
        const tone: TrayRowTone =
          item.kind === 'status_check' ? 'unknown' : 'attention';

        needsHumanCandidates.push({
          priority: ATTENTION_PRIORITY[item.kind],
          updatedAtMs,
          row: {
            key: `tray-attention:${session.key}:${item.key}`,
            category: 'needs_human',
            tone,
            title: item.title,
            projectLabel: project.label,
            chatLabel: session.label,
            statusLabel: formatAttentionStatusLabel(item.kind, item.stateLabel),
            detail: resolveAttentionDetail(
              item.reason,
              linkedJob,
              linkedRequest
            ),
            updatedAtMs,
            ageLabel: formatAgeLabel(updatedAtMs, nowMs),
            threadUuid: verifiedThreadUuid,
          },
        });
      }

      // 2. Working items (counted independently of attention items)
      const runningCalls = session.calls.filter((call) => call.running);
      const runningJobs = session.jobs.filter(
        (jobView) => jobView.job.state === 'running' && !jobView.attention
      );

      const runningJobKeysWithCall = new Set<string>();
      for (const call of runningCalls) {
        const indexed = indexedCalls.get(call.key);
        if (indexed && indexed.jobId !== null) {
          runningJobKeysWithCall.add(
            projectJobLookupKey(indexed.sourceId, indexed.jobId)
          );
        }
      }

      for (const call of runningCalls) {
        workingCount += 1;
        const updatedAtMs = call.startedAtMs ?? session.updatedAtMs;
        const detailParts = [call.role, call.model].filter(
          (part) => part.trim().length > 0 && part !== 'Model not reported'
        );
        workingCandidates.push({
          updatedAtMs,
          row: {
            key: `tray-working-call:${session.key}:${call.key}`,
            category: 'working',
            tone: 'working',
            title: call.title,
            projectLabel: project.label,
            chatLabel: session.label,
            statusLabel: 'Running',
            detail: detailParts.join(' · ') || 'Worker executing',
            updatedAtMs,
            ageLabel: call.durationLabel || formatAgeLabel(updatedAtMs, nowMs),
            threadUuid: verifiedThreadUuid,
          },
        });
      }

      for (const jobView of runningJobs) {
        const jobKey = projectJobLookupKey(
          jobView.job.source_id,
          jobView.job.id
        );
        if (runningJobKeysWithCall.has(jobKey)) {
          continue;
        }
        const jobActiveCalls =
          typeof jobView.job.active_call_count === 'number' &&
          Number.isFinite(jobView.job.active_call_count) &&
          jobView.job.active_call_count > 0
            ? Math.trunc(jobView.job.active_call_count)
            : 1;
        workingCount += jobActiveCalls;
        const updatedAtMs = jobView.updatedAtMs ?? session.updatedAtMs;
        const detail =
          compactFeedText(jobView.job.summary, 140) || jobView.taskSummary;
        workingCandidates.push({
          updatedAtMs,
          row: {
            key: `tray-working-job:${session.key}:${jobView.key}`,
            category: 'working',
            tone: 'working',
            title: trimOrNull(jobView.job.title) ?? session.label,
            projectLabel: project.label,
            chatLabel: session.label,
            statusLabel:
              jobActiveCalls > 1 ? `Running (${jobActiveCalls})` : 'Running',
            detail,
            updatedAtMs,
            ageLabel: formatAgeLabel(updatedAtMs, nowMs),
            threadUuid: verifiedThreadUuid,
          },
        });
      }

      // 3. Meaningful terminal results (Completed / Failed / Cancelled; never relabel Failed or Unknown as Completed)
      const coveredTerminalJobKeys = new Set<string>();
      for (const jobView of session.jobs) {
        if (jobView.attention) {
          continue;
        }
        const state = jobView.job.state;
        if (state !== 'completed' && state !== 'failed') {
          continue;
        }
        const jobKey = projectJobLookupKey(
          jobView.job.source_id,
          jobView.job.id
        );
        coveredTerminalJobKeys.add(jobKey);
        const isFailed = state === 'failed';
        const updatedAtMs = jobView.updatedAtMs ?? session.updatedAtMs;
        resultCandidates.push({
          updatedAtMs,
          row: {
            key: `tray-result-job:${session.key}:${jobView.key}`,
            category: 'result',
            tone: isFailed ? 'danger' : 'success',
            title: trimOrNull(jobView.job.title) ?? session.label,
            projectLabel: project.label,
            chatLabel: session.label,
            statusLabel: isFailed ? 'Failed' : 'Completed',
            detail:
              compactFeedText(jobView.job.summary, 140) || jobView.taskSummary,
            updatedAtMs,
            ageLabel: formatAgeLabel(updatedAtMs, nowMs),
            threadUuid: verifiedThreadUuid,
          },
        });
      }

      const activeOrAttentionJobKeys = new Set<string>();
      for (const jobView of session.jobs) {
        if (jobView.attention || jobView.job.state === 'running') {
          activeOrAttentionJobKeys.add(
            projectJobLookupKey(jobView.job.source_id, jobView.job.id)
          );
        }
      }

      for (const call of session.calls) {
        if (call.running) {
          continue;
        }
        const indexed = indexedCalls.get(call.key);
        const linkedJobKey =
          indexed && indexed.jobId !== null
            ? projectJobLookupKey(indexed.sourceId, indexed.jobId)
            : null;
        if (
          linkedJobKey !== null &&
          (coveredTerminalJobKeys.has(linkedJobKey) ||
            activeOrAttentionJobKeys.has(linkedJobKey))
        ) {
          continue;
        }
        if (
          call.stateLabel !== 'Completed' &&
          call.stateLabel !== 'Failed' &&
          call.stateLabel !== 'Cancelled'
        ) {
          continue;
        }
        const tone: TrayRowTone =
          call.stateLabel === 'Failed'
            ? 'danger'
            : call.stateLabel === 'Cancelled'
              ? 'neutral'
              : 'success';
        const updatedAtMs =
          call.finishedAtMs ?? call.startedAtMs ?? session.updatedAtMs;
        resultCandidates.push({
          updatedAtMs,
          row: {
            key: `tray-result-call:${session.key}:${call.key}`,
            category: 'result',
            tone,
            title: call.title,
            projectLabel: project.label,
            chatLabel: session.label,
            statusLabel: call.stateLabel,
            detail: `${call.role} · ${call.model}`,
            updatedAtMs,
            ageLabel: formatAgeLabel(updatedAtMs, nowMs),
            threadUuid: verifiedThreadUuid,
          },
        });
      }

      if (
        session.jobs.length === 0 &&
        session.calls.length === 0 &&
        session.attentionItems.length === 0 &&
        session.tone === 'danger'
      ) {
        const updatedAtMs = session.updatedAtMs;
        resultCandidates.push({
          updatedAtMs,
          row: {
            key: `tray-result-session:${session.key}`,
            category: 'result',
            tone: 'danger',
            title: session.label,
            projectLabel: project.label,
            chatLabel: session.label,
            statusLabel: 'Failed',
            detail: session.recent[0]?.label ?? 'Execution failed',
            updatedAtMs,
            ageLabel: formatAgeLabel(updatedAtMs, nowMs),
            threadUuid: verifiedThreadUuid,
          },
        });
      }
    }
  }

  needsHumanCandidates.sort((left, right) => {
    if (left.priority !== right.priority) {
      return left.priority - right.priority;
    }
    const leftMs = left.updatedAtMs ?? -1;
    const rightMs = right.updatedAtMs ?? -1;
    if (leftMs !== rightMs) {
      return rightMs - leftMs;
    }
    return compareText(left.row.key, right.row.key);
  });

  workingCandidates.sort((left, right) => {
    const leftMs = left.updatedAtMs ?? -1;
    const rightMs = right.updatedAtMs ?? -1;
    if (leftMs !== rightMs) {
      return rightMs - leftMs;
    }
    return compareText(left.row.key, right.row.key);
  });

  resultCandidates.sort((left, right) => {
    const leftMs = left.updatedAtMs ?? -1;
    const rightMs = right.updatedAtMs ?? -1;
    if (leftMs !== rightMs) {
      return rightMs - leftMs;
    }
    return compareText(left.row.key, right.row.key);
  });

  const mostRecentResult = resultCandidates.slice(0, 1).map((item) => item.row);
  const rows = [
    ...needsHumanCandidates.map((item) => item.row),
    ...workingCandidates.map((item) => item.row),
    ...mostRecentResult,
  ].slice(0, TRAY_MAX_ROWS);

  const headerStatusLabel = formatTrayHeaderSummary(
    workingCount,
    attentionCount
  );

  if (rows.length === 0 && workingCount === 0 && attentionCount === 0) {
    return {
      status: 'empty',
      headerStatusLabel,
      workingCount: 0,
      attentionCount: 0,
      rows: [],
      emptyMessage: 'No active or recent work.',
      unavailableReason: null,
    };
  }

  return {
    status: 'ready',
    headerStatusLabel,
    workingCount,
    attentionCount,
    rows,
    emptyMessage: null,
    unavailableReason: null,
  };
}

export interface TrayCoordinatorState {
  snapshot: ObservationSnapshot | null;
  loading: boolean;
  error: string | null;
}

/**
 * Coordinates tray observation fetches to prevent duplicate in-flight reads,
 * discard stale async responses, and poll only while visible and focused.
 */
export class TrayObservationCoordinator {
  private readonly fetcher: () => Promise<ObservationSnapshot>;
  private readonly onChange?: (state: TrayCoordinatorState) => void;
  private state: TrayCoordinatorState = {
    snapshot: null,
    loading: true,
    error: null,
  };
  private inFlightPromise: Promise<void> | null = null;
  private queuedForceRefresh = false;
  private requestSeq = 0;
  private latestIssuedSeq = 0;
  private visible = true;
  private focused = true;
  private disposed = false;

  public constructor(
    fetcher: () => Promise<ObservationSnapshot>,
    onChange?: (state: TrayCoordinatorState) => void
  ) {
    this.fetcher = fetcher;
    this.onChange = onChange;
  }

  public getState(): TrayCoordinatorState {
    return { ...this.state };
  }

  public isInFlight(): boolean {
    return this.inFlightPromise !== null;
  }

  public setVisibility(visible: boolean, focused: boolean): void {
    this.visible = visible;
    this.focused = focused;
  }

  public canPoll(): boolean {
    return !this.disposed && this.visible && this.focused;
  }

  /**
   * Triggers a snapshot refresh.
   * - If a read is already in flight and `force` is false, coalesces onto the
   *   existing in-flight read without starting a second IPC request.
   * - If `force` is true (e.g. `threadlane:tray-opened`) while a read is in
   *   flight, queues exactly one follow-up read and invalidates the older sequence.
   */
  public refresh(options: { force?: boolean } = {}): Promise<void> {
    if (this.disposed) {
      return Promise.resolve();
    }
    if (this.inFlightPromise !== null) {
      if (options.force) {
        this.queuedForceRefresh = true;
        this.latestIssuedSeq = ++this.requestSeq;
      }
      return this.inFlightPromise;
    }
    return this.startRequest();
  }

  public dispose(): void {
    this.disposed = true;
    this.queuedForceRefresh = false;
    this.latestIssuedSeq = ++this.requestSeq;
  }

  private startRequest(): Promise<void> {
    const seq = ++this.requestSeq;
    this.latestIssuedSeq = seq;
    if (this.state.snapshot === null && !this.state.loading) {
      this.state = {
        ...this.state,
        loading: true,
      };
      this.emitChange();
    }

    const run = async (): Promise<void> => {
      try {
        const snapshot = await this.fetcher();
        if (this.disposed || seq !== this.latestIssuedSeq) {
          return;
        }
        this.state = {
          snapshot,
          loading: false,
          error: null,
        };
        this.emitChange();
      } catch (err) {
        if (this.disposed || seq !== this.latestIssuedSeq) {
          return;
        }
        this.state = {
          snapshot: null,
          loading: false,
          error: normalizeTrayErrorMessage(
            err,
            'Observation status is unavailable.'
          ),
        };
        this.emitChange();
      } finally {
        this.inFlightPromise = null;
        if (!this.disposed && this.queuedForceRefresh) {
          this.queuedForceRefresh = false;
          await this.startRequest();
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
