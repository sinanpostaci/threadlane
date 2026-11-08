/**
 * Pure versioned cutoff persistence and pre-projection snapshot filtering for
 * the reversible Projects soft-archive view filter.
 *
 * This module has no DOM, Tauri, shell, or provider imports so cutoff
 * validation, storage error handling, and immutable snapshot filtering can be
 * tested directly under node:test.
 */
import type {
  ObservationCall,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
} from '../observation/types.ts';

export const PROJECTS_ARCHIVE_STORAGE_KEY = 'threadlane.projects.archive.v1';
export const PROJECTS_ARCHIVE_SCHEMA_VERSION = 1 as const;

export interface ProjectsArchiveStorageReader {
  getItem(key: string): string | null;
}

export interface ProjectsArchiveStorage extends ProjectsArchiveStorageReader {
  setItem(key: string, value: string): void;
}

export interface ProjectsArchiveRecordV1 {
  version: typeof PROJECTS_ARCHIVE_SCHEMA_VERSION;
  archive_before_ms: number;
}

export type ProjectsArchiveWriteResult =
  | {
      ok: true;
      archiveBeforeMs: number;
    }
  | {
      ok: false;
      error: string;
    };

function isValidRecordedTimestampMs(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value > 0;
}

function scopedRecordKey(sourceId: string, id: string): string {
  return `${sourceId.length}:${sourceId}:${id}`;
}

export function sanitizeProjectsArchiveCutoffMs(
  value: unknown,
  nowMs: number = Date.now()
): number | null {
  if (
    typeof value !== 'number' ||
    !Number.isFinite(value) ||
    !Number.isSafeInteger(value) ||
    value <= 0
  ) {
    return null;
  }
  if (typeof nowMs !== 'number' || !Number.isFinite(nowMs) || nowMs <= 0) {
    return null;
  }
  if (value > nowMs) {
    return null;
  }
  return value;
}

export function parseProjectsArchiveCutoff(
  raw: string | null | undefined,
  nowMs: number = Date.now()
): number | null {
  if (typeof raw !== 'string' || raw.trim().length === 0) {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return null;
  }
  const record = parsed as Record<string, unknown>;
  if (record.version !== PROJECTS_ARCHIVE_SCHEMA_VERSION) {
    return null;
  }
  return sanitizeProjectsArchiveCutoffMs(record.archive_before_ms, nowMs);
}

export function readProjectsArchiveCutoff(
  storage: ProjectsArchiveStorageReader | null | undefined,
  nowMs: number = Date.now()
): number | null {
  if (!storage || typeof storage.getItem !== 'function') {
    return null;
  }
  try {
    const raw = storage.getItem(PROJECTS_ARCHIVE_STORAGE_KEY);
    return parseProjectsArchiveCutoff(raw, nowMs);
  } catch {
    return null;
  }
}

export function writeProjectsArchiveCutoff(
  storage: ProjectsArchiveStorage | null | undefined,
  cutoffMs: unknown,
  nowMs: number = Date.now()
): ProjectsArchiveWriteResult {
  const sanitized = sanitizeProjectsArchiveCutoffMs(cutoffMs, nowMs);
  if (sanitized === null) {
    return {
      ok: false,
      error:
        'Unable to archive Projects history because the cutoff timestamp is invalid.',
    };
  }
  if (!storage || typeof storage.setItem !== 'function') {
    return {
      ok: false,
      error:
        'Unable to save Projects archive cutoff because local storage is unavailable.',
    };
  }
  const payload: ProjectsArchiveRecordV1 = {
    version: PROJECTS_ARCHIVE_SCHEMA_VERSION,
    archive_before_ms: sanitized,
  };
  try {
    storage.setItem(PROJECTS_ARCHIVE_STORAGE_KEY, JSON.stringify(payload));
    return {
      ok: true,
      archiveBeforeMs: sanitized,
    };
  } catch {
    return {
      ok: false,
      error: 'Unable to save Projects archive cutoff in local storage.',
    };
  }
}

function isCallActive(call: ObservationCall): boolean {
  return call.state === 'running' || call.state === 'queued';
}

function shouldRetainCall(call: ObservationCall, cutoffMs: number): boolean {
  if (isCallActive(call)) {
    return true;
  }
  const startedAtMs = isValidRecordedTimestampMs(call.started_at_ms)
    ? call.started_at_ms
    : null;
  const finishedAtMs = isValidRecordedTimestampMs(call.finished_at_ms)
    ? call.finished_at_ms
    : null;
  if (startedAtMs === null && finishedAtMs === null) {
    return true;
  }
  const latestMs = Math.max(
    startedAtMs ?? -Infinity,
    finishedAtMs ?? -Infinity
  );
  return latestMs > cutoffMs;
}

function isJobActive(job: ObservationJob): boolean {
  if (job.state === 'running') {
    return true;
  }
  return (
    typeof job.active_call_count === 'number' &&
    Number.isFinite(job.active_call_count) &&
    job.active_call_count > 0
  );
}

function shouldRetainJobDirectly(
  job: ObservationJob,
  cutoffMs: number
): boolean {
  if (isJobActive(job)) {
    return true;
  }
  const updatedAtMs = job.updated_at_ms;
  if (!isValidRecordedTimestampMs(updatedAtMs)) {
    return true;
  }
  return updatedAtMs > cutoffMs;
}

function isRequestActive(request: ObservationRequest): boolean {
  return request.state === 'running' || request.state === 'queued';
}

function shouldRetainRequestByOwnTimestamps(
  request: ObservationRequest,
  cutoffMs: number
): boolean {
  let hasValidTimestamp = false;
  let latestMs = -Infinity;

  const recordTimestamp = (candidate: unknown): void => {
    if (isValidRecordedTimestampMs(candidate)) {
      hasValidTimestamp = true;
      if (candidate > latestMs) {
        latestMs = candidate;
      }
    }
  };

  recordTimestamp(request.started_at_ms);
  recordTimestamp(request.finished_at_ms);
  recordTimestamp(request.updated_at_ms);
  for (const delivery of request.delivery ?? []) {
    recordTimestamp(delivery.at_ms);
  }

  if (!hasValidTimestamp) {
    return true;
  }
  return latestMs > cutoffMs;
}

export function filterArchivedObservationSnapshot(
  snapshot: ObservationSnapshot,
  archiveBeforeMs: number | null | undefined,
  nowMs?: number
): ObservationSnapshot {
  const referenceNowMs = isValidRecordedTimestampMs(nowMs)
    ? nowMs
    : isValidRecordedTimestampMs(snapshot.generated_at_ms)
      ? snapshot.generated_at_ms
      : Date.now();
  const cutoffMs = sanitizeProjectsArchiveCutoffMs(
    archiveBeforeMs,
    referenceNowMs
  );
  if (cutoffMs === null) {
    return {
      ...snapshot,
      sources: [...(snapshot.sources ?? [])],
      jobs: [...(snapshot.jobs ?? [])],
      requests: (snapshot.requests ?? []).map((request) => ({
        ...request,
        children: [...(request.children ?? [])],
      })),
      warnings: [...(snapshot.warnings ?? [])],
    };
  }

  const jobs = snapshot.jobs ?? [];
  const requests = snapshot.requests ?? [];

  const retainedJobKeys = new Set<string>();
  for (const job of jobs) {
    if (shouldRetainJobDirectly(job, cutoffMs)) {
      retainedJobKeys.add(scopedRecordKey(job.source_id, job.id));
    }
  }

  const requestsByKey = new Map<string, ObservationRequest[]>();
  const retainedChildrenByRequest = new Map<
    ObservationRequest,
    ObservationCall[]
  >();
  for (const request of requests) {
    const reqKey = scopedRecordKey(request.source_id, request.id);
    const bucket = requestsByKey.get(reqKey);
    if (bucket) {
      bucket.push(request);
    } else {
      requestsByKey.set(reqKey, [request]);
    }
    const originalChildren = request.children ?? [];
    retainedChildrenByRequest.set(
      request,
      originalChildren.filter((call) => shouldRetainCall(call, cutoffMs))
    );
  }

  const retainedRequestKeys = new Set<string>();
  const ancestorQueue: ObservationRequest[] = [];
  const markRequestKeyRetained = (reqKey: string): void => {
    if (retainedRequestKeys.has(reqKey)) {
      return;
    }
    const matching = requestsByKey.get(reqKey);
    if (!matching || matching.length === 0) {
      return;
    }
    retainedRequestKeys.add(reqKey);
    for (const req of matching) {
      ancestorQueue.push(req);
    }
  };

  for (const request of requests) {
    const reqKey = scopedRecordKey(request.source_id, request.id);
    const retainedChildren = retainedChildrenByRequest.get(request) ?? [];

    if (
      isRequestActive(request) ||
      retainedChildren.length > 0 ||
      shouldRetainRequestByOwnTimestamps(request, cutoffMs)
    ) {
      markRequestKeyRetained(reqKey);
    }
  }

  while (ancestorQueue.length > 0) {
    const current = ancestorQueue.pop()!;
    if (
      typeof current.parent_request_id === 'string' &&
      current.parent_request_id.length > 0
    ) {
      markRequestKeyRetained(
        scopedRecordKey(current.source_id, current.parent_request_id)
      );
    }
  }

  for (const request of requests) {
    const reqKey = scopedRecordKey(request.source_id, request.id);
    if (!retainedRequestKeys.has(reqKey)) {
      continue;
    }
    if (typeof request.job_id === 'string' && request.job_id.length > 0) {
      retainedJobKeys.add(scopedRecordKey(request.source_id, request.job_id));
    }
  }

  const filteredJobs = jobs.filter((job) =>
    retainedJobKeys.has(scopedRecordKey(job.source_id, job.id))
  );

  const filteredRequests: ObservationRequest[] = [];
  for (const request of requests) {
    const reqKey = scopedRecordKey(request.source_id, request.id);
    if (!retainedRequestKeys.has(reqKey)) {
      continue;
    }
    const originalChildren = request.children ?? [];
    const retainedChildren = retainedChildrenByRequest.get(request) ?? [];
    const removedCount = originalChildren.length - retainedChildren.length;
    const nextChildrenCount =
      removedCount > 0 && request.children_count > 0
        ? Math.max(
            retainedChildren.length,
            request.children_count - removedCount
          )
        : request.children_count;

    filteredRequests.push({
      ...request,
      children: retainedChildren,
      children_count: nextChildrenCount,
    });
  }

  return {
    ...snapshot,
    sources: [...(snapshot.sources ?? [])],
    jobs: filteredJobs,
    requests: filteredRequests,
    warnings: [...(snapshot.warnings ?? [])],
  };
}
