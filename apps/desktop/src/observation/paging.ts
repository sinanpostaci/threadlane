/**
 * DOM-free collection of bounded observation request windows.
 *
 * The observation bridge pages newest-first and treats cursors as opaque. This
 * helper follows only the bridge's real `next_cursor` and `has_more` values; it
 * never synthesizes cursors and stops immediately if the bridge repeats one.
 */
import type {
  ObservationRequest,
  ObservationSnapshot,
  ObservationWarningContext,
} from './types.ts';

export const OBSERVATION_DEFAULT_PAGE_LIMIT = 25;
export const OBSERVATION_MAX_PAGE_LIMIT = 200;
export const OBSERVATION_MAX_WINDOW_REQUESTS = 1000;
export const OBSERVATION_MAX_WARNING_CONTEXTS = 32;

export interface ObservationPageQuery {
  cursor: string | null;
  limit: number;
}

export type ObservationPageFetcher = (
  query: ObservationPageQuery
) => Promise<ObservationSnapshot>;

export interface ObservationWindowOptions {
  fetcher: ObservationPageFetcher;
  target: number;
  pageLimit?: number;
  maxRequests?: number;
  shouldContinue?: () => boolean;
}

export interface ObservationWindowResult {
  snapshot: ObservationSnapshot | null;
  aborted: boolean;
  pagesFetched: number;
  requestsCollected: number;
  target: number;
  capped: boolean;
  exhausted: boolean;
  warnings: string[];
}

function positiveInteger(value: number, fallback: number): number {
  return Number.isFinite(value) ? Math.max(1, Math.trunc(value)) : fallback;
}

function normalizeCursor(cursor: string | null | undefined): string | null {
  return typeof cursor === 'string' && cursor.length > 0 ? cursor : null;
}

function observationRequestKey(request: ObservationRequest): string {
  return `${request.source_id}\u0000${request.id}`;
}

function observationRequestFreshness(request: ObservationRequest): number {
  let newest = 0;
  const candidates = [
    request.updated_at_ms,
    request.finished_at_ms,
    request.started_at_ms,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === 'number' && Number.isFinite(candidate) && candidate > newest) {
      newest = candidate;
    }
  }
  return newest;
}

function appendWarning(warnings: string[], warning: string | null | undefined): void {
  const trimmed = (warning ?? '').trim();
  if (trimmed.length > 0 && !warnings.includes(trimmed)) {
    warnings.push(trimmed);
  }
}

function normalizeWarningOrigin(origin: string | null | undefined): string {
  const trimmed = (origin ?? '').trim();
  if (
    trimmed === 'registry' ||
    trimmed === 'jobs' ||
    trimmed === 'journal' ||
    trimmed === 'requests' ||
    trimmed === 'snapshot'
  ) {
    return trimmed;
  }
  return 'snapshot';
}

function normalizeWarningSourceId(sourceId: string | null | undefined): string | null {
  if (typeof sourceId !== 'string') {
    return null;
  }
  const trimmed = sourceId.trim();
  if (
    trimmed.length === 0 ||
    trimmed.length > 128 ||
    trimmed !== sourceId ||
    !/^[A-Za-z0-9_.-]+$/.test(trimmed)
  ) {
    return null;
  }
  return trimmed;
}

function appendWarningContext(
  contexts: ObservationWarningContext[],
  seenKeys: Set<string>,
  raw: ObservationWarningContext | null | undefined
): void {
  if (!raw || typeof raw !== 'object') {
    return;
  }
  const warning = typeof raw.warning === 'string' ? raw.warning.trim() : '';
  if (warning.length === 0) {
    return;
  }
  const sourceId = normalizeWarningSourceId(raw.source_id);
  const origin = normalizeWarningOrigin(raw.origin);
  const key = `${warning}\u0000${sourceId ?? ''}\u0000${origin}`;
  if (!seenKeys.has(key)) {
    seenKeys.add(key);
    contexts.push({
      warning,
      source_id: sourceId,
      origin,
    });
  }
}

/**
 * Collects a fresh head window of unique requests.
 *
 * The first page supplies the current jobs and sources. Requests are keyed by
 * exact source id plus request id, so identical request ids from different
 * sources remain distinct. When a duplicate appears on a later page, the newer
 * representation wins but keeps the position of its first occurrence.
 */
export async function collectObservationWindow(
  options: ObservationWindowOptions
): Promise<ObservationWindowResult> {
  const maxRequests = positiveInteger(
    options.maxRequests ?? OBSERVATION_MAX_WINDOW_REQUESTS,
    OBSERVATION_MAX_WINDOW_REQUESTS
  );
  const requestedTarget = Number.isFinite(options.target)
    ? Math.max(0, Math.trunc(options.target))
    : OBSERVATION_DEFAULT_PAGE_LIMIT;
  const target = Math.min(requestedTarget, maxRequests);
  const pageLimit = Math.min(
    OBSERVATION_MAX_PAGE_LIMIT,
    positiveInteger(
      options.pageLimit ?? OBSERVATION_MAX_PAGE_LIMIT,
      OBSERVATION_MAX_PAGE_LIMIT
    )
  );
  const shouldContinue = options.shouldContinue ?? (() => true);
  // Permit normal paging plus limited overlap without a long empty-page loop.
  const maxPages = Math.max(1, Math.min(maxRequests, Math.ceil(maxRequests / pageLimit) + 2));
  const requests = new Map<string, ObservationRequest>();
  const warnings: string[] = [];
  const rawWarningContexts: ObservationWarningContext[] = [];
  const seenWarningContextKeys = new Set<string>();
  const seenCursors = new Set<string>();
  let sawWarningContexts = false;
  let cursor: string | null = null;
  let firstPage: ObservationSnapshot | null = null;
  let lastPage: ObservationSnapshot | null = null;
  let pagesFetched = 0;
  let exhausted = false;
  let cappedBy: 'requests' | 'pages' | null = null;
  let repeatedCursor: string | null = null;

  while (shouldContinue()) {
    if (pagesFetched > 0) {
      if (requests.size >= target) {
        break;
      }
      if (pagesFetched >= maxPages) {
        cappedBy = 'pages';
        break;
      }
      if (requests.size >= maxRequests) {
        cappedBy = 'requests';
        break;
      }
    }

    const remainingCapacity = maxRequests - requests.size;
    if (remainingCapacity <= 0) {
      cappedBy = 'requests';
      break;
    }
    const targetRemaining = Math.max(0, target - requests.size);
    const desiredSize =
      targetRemaining > 0
        ? Math.max(OBSERVATION_DEFAULT_PAGE_LIMIT, targetRemaining)
        : OBSERVATION_DEFAULT_PAGE_LIMIT;
    const limit = Math.max(1, Math.min(pageLimit, desiredSize, remainingCapacity));

    const page = await options.fetcher({ cursor, limit });
    pagesFetched += 1;
    if (!shouldContinue()) {
      return {
        snapshot: null,
        aborted: true,
        pagesFetched,
        requestsCollected: requests.size,
        target,
        capped: false,
        exhausted: false,
        warnings: [],
      };
    }

    firstPage ??= page;
    lastPage = page;
    for (const warning of page.warnings ?? []) {
      appendWarning(warnings, warning);
    }
    if (Array.isArray(page.warning_contexts)) {
      sawWarningContexts = true;
      for (const context of page.warning_contexts) {
        appendWarningContext(rawWarningContexts, seenWarningContextKeys, context);
      }
    }
    for (const request of page.requests ?? []) {
      const key = observationRequestKey(request);
      const existing = requests.get(key);
      if (
        !existing ||
        observationRequestFreshness(request) > observationRequestFreshness(existing)
      ) {
        requests.set(key, request);
      }
    }

    const nextCursor = normalizeCursor(page.next_cursor);
    if (page.has_more !== true) {
      exhausted = true;
      break;
    }
    if (nextCursor === null) {
      exhausted = true;
      appendWarning(
        warnings,
        'Activity paging stopped because the server reported more requests without a next cursor.'
      );
      break;
    }
    if (nextCursor === cursor || seenCursors.has(nextCursor)) {
      repeatedCursor = nextCursor;
      appendWarning(
        warnings,
        `Activity paging stopped because the server repeated cursor "${nextCursor}".`
      );
      break;
    }
    seenCursors.add(nextCursor);
    cursor = nextCursor;

    if (requests.size >= maxRequests) {
      cappedBy = 'requests';
      break;
    }
  }

  if (!shouldContinue()) {
    return {
      snapshot: null,
      aborted: true,
      pagesFetched,
      requestsCollected: requests.size,
      target,
      capped: false,
      exhausted: false,
      warnings: [],
    };
  }

  if (!firstPage) {
    return {
      snapshot: null,
      aborted: false,
      pagesFetched,
      requestsCollected: 0,
      target,
      capped: false,
      exhausted: false,
      warnings: [],
    };
  }

  const finalPage = lastPage ?? firstPage;
  const nextCursor = repeatedCursor
    ? null
    : normalizeCursor(finalPage.next_cursor);

  if (cappedBy === 'pages') {
    const continuation =
      nextCursor === null
        ? ''
        : ' Use Load more to continue from the returned cursor.';
    appendWarning(
      warnings,
      `Activity paging stopped after ${maxPages} page requests before the requested window was complete.${continuation}`
    );
  }
  if (cappedBy === 'requests') {
    const continuation =
      nextCursor === null
        ? ''
        : ' Use Load more to continue from the returned cursor.';
    appendWarning(
      warnings,
      `Activity paging reached the ${maxRequests}-request window limit. Loaded ${requests.size} requests; older activity was not loaded.${continuation}`
    );
  }

  const survivingWarnings = new Set(warnings);
  const warningContexts = rawWarningContexts
    .filter((context) => survivingWarnings.has(context.warning))
    .slice(0, OBSERVATION_MAX_WARNING_CONTEXTS);

  const mergedSnapshot: ObservationSnapshot = {
    ...firstPage,
    requests: [...requests.values()],
    warnings: [...warnings],
    next_cursor: nextCursor,
    has_more: repeatedCursor === null && finalPage.has_more === true,
    jobs_has_more: firstPage.jobs_has_more === true,
  };
  if (sawWarningContexts || warningContexts.length > 0) {
    mergedSnapshot.warning_contexts = warningContexts;
  }

  return {
    snapshot: mergedSnapshot,
    aborted: false,
    pagesFetched,
    requestsCollected: requests.size,
    target,
    capped: cappedBy !== null,
    exhausted,
    warnings: [...warnings],
  };
}
