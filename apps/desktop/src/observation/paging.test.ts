import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type {
  ObservationActor,
  ObservationDelivery,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
  ObservationUsage,
} from './types.ts';
import {
  collectObservationWindow,
  OBSERVATION_MAX_PAGE_LIMIT,
  OBSERVATION_MAX_WINDOW_REQUESTS,
  type ObservationPageFetcher,
  type ObservationPageQuery,
} from './paging.ts';

const NOW_MS = 1_730_000_000_000;

function makeSource(overrides: Partial<ObservationSource> = {}): ObservationSource {
  return {
    id: 'source-a',
    label: 'Source A',
    project_path: '/tmp/source-a',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeActor(overrides: Partial<ObservationActor> = {}): ObservationActor {
  return {
    label: 'Operator',
    role: 'operator',
    source: 'CLI',
    model: null,
    effort: null,
    evidence: 'operator',
    ...overrides,
  };
}

function makeExecutor(overrides: Partial<ObservationExecutor> = {}): ObservationExecutor {
  return {
    tool: 'codex',
    provider: 'openai',
    role: 'coder',
    requested_model: 'gpt-5',
    observed_model: 'gpt-5',
    requested_effort: 'high',
    observed_effort: 'high',
    ...overrides,
  };
}

function makeUsage(overrides: Partial<ObservationUsage> = {}): ObservationUsage {
  return {
    input_tokens: 100,
    output_tokens: 20,
    cache_read_tokens: 5,
    total_tokens: 125,
    provenance: 'provider',
    ...overrides,
  };
}

function makeDelivery(overrides: Partial<ObservationDelivery> = {}): ObservationDelivery {
  return {
    stage: 'response_returned',
    at_ms: NOW_MS,
    summary: 'Response returned to the origin.',
    recipient: 'operator',
    ...overrides,
  };
}

function makeRequest(overrides: Partial<ObservationRequest> = {}): ObservationRequest {
  return {
    source_id: 'source-a',
    id: 'request-1',
    job_id: 'job-1',
    parent_request_id: '',
    title: 'Recorded request',
    operation: 'execute',
    project_label: 'Project A',
    binding_id: 'binding-a',
    thread_uuid: 'thread-a',
    thread_label: 'Thread A',
    state: 'completed',
    started_at_ms: NOW_MS - 2_000,
    finished_at_ms: NOW_MS,
    updated_at_ms: NOW_MS,
    duration_ms: 2_000,
    actor: makeActor(),
    executor: makeExecutor(),
    usage: makeUsage(),
    delivery: [makeDelivery()],
    children: [],
    children_count: 0,
    summary: 'Recorded request summary',
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'source-a',
    id: 'job-1',
    project_label: 'Project A',
    project_path: '/tmp/project-a',
    title: 'Job One',
    thread_uuid: 'thread-a',
    thread_label: 'Thread A',
    binding_id: 'binding-a',
    plan_id: 'plan-a',
    plan_revision: 1,
    state: 'completed',
    approval_status: 'approved',
    execution_status: 'completed',
    review_state: 'approved',
    updated_at_ms: NOW_MS,
    active_call_count: 0,
    tasks: [],
    summary: 'Job summary',
    ...overrides,
  };
}

function makeSnapshot(overrides: Partial<ObservationSnapshot> = {}): ObservationSnapshot {
  return {
    schema_version: 1,
    generated_at_ms: NOW_MS,
    sources: [makeSource()],
    jobs: [makeJob()],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    ...overrides,
  };
}

interface FakeServer {
  requests: ObservationRequest[];
  sources?: ObservationSource[];
  jobs?: ObservationJob[];
  warnings?: string[];
}

function offsetCursor(cursor: string | null): number {
  if (cursor === null) {
    return 0;
  }
  const match = /^offset-(\d+)$/.exec(cursor);
  return match ? Number(match[1]) : 0;
}

function makeOffsetFetcher(
  server: FakeServer,
  log: ObservationPageQuery[] = []
): ObservationPageFetcher {
  return async ({ cursor, limit }) => {
    log.push({ cursor, limit });
    const start = offsetCursor(cursor);
    const slice = server.requests.slice(start, start + limit);
    const nextOffset = start + slice.length;
    const hasMore = nextOffset < server.requests.length;
    return makeSnapshot({
      sources: server.sources ?? [makeSource()],
      jobs: server.jobs ?? [makeJob()],
      requests: slice,
      warnings: server.warnings ?? [],
      next_cursor: hasMore ? `offset-${nextOffset}` : null,
      has_more: hasMore,
    });
  };
}

function makeRequests(count: number, prefix: string = 'request'): ObservationRequest[] {
  return Array.from({ length: count }, (_, index) =>
    makeRequest({
      id: `${prefix}-${String(index).padStart(3, '0')}`,
      updated_at_ms: NOW_MS - index * 1_000,
      started_at_ms: NOW_MS - index * 1_000 - 2_000,
      finished_at_ms: NOW_MS - index * 1_000,
    })
  );
}

describe('observation paging window', () => {
  it('refreshes a fresh head window that preserves the loaded polling depth', async () => {
    const server: FakeServer = { requests: makeRequests(90) };
    const log: ObservationPageQuery[] = [];
    const fetcher = makeOffsetFetcher(server, log);

    const initial = await collectObservationWindow({
      target: 25,
      pageLimit: 25,
      fetcher,
    });
    assert.equal(initial.aborted, false);
    assert.equal(initial.requestsCollected, 25);
    assert.equal(initial.snapshot?.requests.length, 25);
    assert.equal(initial.snapshot?.next_cursor, 'offset-25');
    assert.equal(initial.snapshot?.has_more, true);
    assert.deepEqual(log.map((query) => query.cursor), [null]);

    const deeper = await collectObservationWindow({
      target: 75,
      pageLimit: 25,
      fetcher,
    });
    assert.equal(deeper.requestsCollected, 75);
    assert.equal(deeper.pagesFetched, 3);
    assert.deepEqual(deeper.snapshot?.requests.map((request) => request.id), [
      ...makeRequests(75).map((request) => request.id),
    ]);
    log.length = 0;

    const refreshed = await collectObservationWindow({
      target: deeper.requestsCollected,
      pageLimit: 25,
      fetcher,
    });
    assert.equal(refreshed.requestsCollected, 75);
    assert.equal(refreshed.pagesFetched, 3);
    assert.equal(refreshed.snapshot?.requests.length, 75);
    assert.equal(refreshed.snapshot?.next_cursor, 'offset-75');
    assert.deepEqual(log.map((query) => query.cursor), [null, 'offset-25', 'offset-50']);
    assert.deepEqual(log.map((query) => query.limit), [25, 25, 25]);
  });

  it('reflects completed and removed live requests instead of retaining stale rows', async () => {
    const server: FakeServer = {
      requests: [
        makeRequest({ id: 'live-request', state: 'running', finished_at_ms: null }),
        makeRequest({ id: 'removed-request', state: 'running', finished_at_ms: null }),
        ...makeRequests(30, 'older'),
      ],
      sources: [makeSource({ label: 'Initial source' })],
      jobs: [makeJob({ title: 'Initial job' })],
    };
    const fetcher = makeOffsetFetcher(server);

    const initial = await collectObservationWindow({ target: 25, fetcher });
    assert.equal(initial.requestsCollected, 25);
    assert.equal(
      initial.snapshot?.requests.find((request) => request.id === 'live-request')?.state,
      'running'
    );
    assert.ok(
      initial.snapshot?.requests.some((request) => request.id === 'removed-request')
    );

    server.requests = server.requests
      .filter((request) => request.id !== 'removed-request')
      .map((request) =>
        request.id === 'live-request'
          ? makeRequest({
              id: request.id,
              state: 'completed',
              started_at_ms: request.started_at_ms,
              finished_at_ms: NOW_MS,
              updated_at_ms: NOW_MS,
            })
          : request
      );
    server.sources = [makeSource({ label: 'Fresh source' })];
    server.jobs = [makeJob({ title: 'Fresh job' })];

    const refreshed = await collectObservationWindow({
      target: initial.requestsCollected,
      fetcher,
    });
    const refreshedRequests = refreshed.snapshot?.requests ?? [];
    assert.equal(refreshed.requestsCollected, initial.requestsCollected);
    assert.equal(
      refreshedRequests.find((request) => request.id === 'live-request')?.state,
      'completed'
    );
    assert.equal(
      refreshedRequests.some((request) => request.id === 'removed-request'),
      false
    );
    assert.equal(refreshedRequests.some((request) => request.state === 'running'), false);
    assert.equal(refreshed.snapshot?.sources[0]?.label, 'Fresh source');
    assert.equal(refreshed.snapshot?.jobs[0]?.title, 'Fresh job');
  });

  it('keeps same request ids from different sources distinct and deduplicates exact identity', async () => {
    const duplicateOlder = makeRequest({
      id: 'shared-request',
      source_id: 'source-a',
      title: 'Alpha first copy',
      updated_at_ms: NOW_MS - 5_000,
      started_at_ms: NOW_MS - 6_000,
      finished_at_ms: NOW_MS - 5_000,
      state: 'running',
    });
    const duplicateNewer = makeRequest({
      id: 'shared-request',
      source_id: 'source-a',
      title: 'Alpha newer copy',
      updated_at_ms: NOW_MS,
      state: 'completed',
    });
    const otherSource = makeRequest({
      id: 'shared-request',
      source_id: 'source-b',
      title: 'Beta request',
    });
    const server: FakeServer = {
      requests: [
        duplicateOlder,
        otherSource,
        ...makeRequests(23),
        duplicateNewer,
        ...makeRequests(5, 'tail'),
      ],
    };
    const log: ObservationPageQuery[] = [];
    const fetcher = makeOffsetFetcher(server, log);

    const result = await collectObservationWindow({
      target: 30,
      pageLimit: 25,
      fetcher,
    });
    const requests = result.snapshot?.requests ?? [];
    const shared = requests.filter((request) => request.id === 'shared-request');
    assert.equal(shared.length, 2);
    assert.deepEqual(
      new Set(shared.map((request) => request.source_id)),
      new Set(['source-a', 'source-b'])
    );
    const alpha = shared.find((request) => request.source_id === 'source-a');
    assert.equal(alpha?.state, 'completed');
    assert.equal(alpha?.title, 'Alpha newer copy');
    assert.equal(result.requestsCollected, 30);
    assert.equal(requests.length, 30);
    assert.equal(log.length, 2);
  });

  it('abandons the window as soon as the scope is no longer current', async () => {
    let fetches = 0;
    let checks = 0;
    const fetcher: ObservationPageFetcher = async () => {
      fetches += 1;
      return makeSnapshot({
        requests: makeRequests(25),
        next_cursor: 'offset-25',
        has_more: true,
      });
    };

    const result = await collectObservationWindow({
      target: 50,
      pageLimit: 25,
      fetcher,
      shouldContinue: () => {
        checks += 1;
        return checks <= 1;
      },
    });

    assert.equal(fetches, 1);
    assert.equal(result.aborted, true);
    assert.equal(result.snapshot, null);
    assert.equal(result.pagesFetched, 1);
  });

  it('stops on a repeated cursor with an explicit warning and no dangling cursor', async () => {
    let fetches = 0;
    const fetcher: ObservationPageFetcher = async ({ cursor }) => {
      fetches += 1;
      return makeSnapshot({
        requests: [makeRequest({ id: `request-${cursor ?? 'head'}` })],
        next_cursor: 'repeated-cursor',
        has_more: true,
        warnings: ['Page warning'],
      });
    };

    const result = await collectObservationWindow({
      target: 100,
      pageLimit: 25,
      fetcher,
    });

    assert.equal(fetches, 2);
    assert.equal(result.aborted, false);
    assert.equal(result.snapshot?.requests.length, 2);
    assert.equal(result.snapshot?.next_cursor, null);
    assert.equal(result.snapshot?.has_more, false);
    assert.match(result.warnings.join(' '), /repeated cursor/i);
    assert.deepEqual(result.snapshot?.warnings, result.warnings);
  });

  it('caps a refresh at the request window limit while keeping a valid continuation cursor', async () => {
    const server: FakeServer = { requests: makeRequests(1_200) };
    const log: ObservationPageQuery[] = [];
    const fetcher = makeOffsetFetcher(server, log);

    const result = await collectObservationWindow({
      target: OBSERVATION_MAX_WINDOW_REQUESTS,
      fetcher,
    });

    assert.equal(result.capped, true);
    assert.equal(result.requestsCollected, OBSERVATION_MAX_WINDOW_REQUESTS);
    assert.equal(result.snapshot?.requests.length, OBSERVATION_MAX_WINDOW_REQUESTS);
    assert.equal(result.pagesFetched, 5);
    assert.deepEqual(log.map((query) => query.limit), [200, 200, 200, 200, 200]);
    assert.ok(log.every((query) => query.limit <= OBSERVATION_MAX_PAGE_LIMIT));
    assert.equal(result.snapshot?.next_cursor, 'offset-1000');
    assert.equal(result.snapshot?.has_more, true);
    const warning = result.warnings.join(' ');
    assert.match(warning, /1000-request window limit/);
    assert.match(warning, /Load more/);

    const continuation = await fetcher({ cursor: 'offset-1000', limit: 25 });
    assert.equal(continuation.requests[0]?.id, 'request-1000');
    assert.equal(continuation.next_cursor, 'offset-1025');
  });

  it('bounds duplicate-only pages without creating duplicate rows or an infinite loop', async () => {
    const duplicate = makeRequest({ id: 'same-request' });
    let fetches = 0;
    const fetcher: ObservationPageFetcher = async () => {
      fetches += 1;
      return makeSnapshot({
        requests: [duplicate],
        next_cursor: `cursor-${fetches}`,
        has_more: true,
      });
    };

    const result = await collectObservationWindow({
      target: 10,
      pageLimit: 5,
      maxRequests: 5,
      fetcher,
    });

    assert.equal(fetches, 3);
    assert.equal(result.capped, true);
    assert.equal(result.requestsCollected, 1);
    assert.equal(result.snapshot?.requests.length, 1);
    assert.equal(result.snapshot?.requests[0]?.id, 'same-request');
    assert.match(result.warnings.join(' '), /stopped after 3 page requests/i);
  });

  it('merges warnings from every page without duplicates and keeps head jobs/sources', async () => {
    const fetcher: ObservationPageFetcher = async ({ cursor }) => {
      const first = cursor === null;
      return makeSnapshot({
        sources: [makeSource({ label: first ? 'Head source' : 'Later source' })],
        jobs: [makeJob({ title: first ? 'Head job' : 'Later job' })],
        requests: makeRequests(first ? 25 : 5, first ? 'head' : 'later'),
        warnings: first
          ? ['Shared warning', 'Head warning']
          : ['Shared warning', 'Later warning'],
        next_cursor: first ? 'offset-25' : null,
        has_more: first,
      });
    };

    const result = await collectObservationWindow({
      target: 30,
      pageLimit: 25,
      fetcher,
    });

    assert.equal(result.snapshot?.sources[0]?.label, 'Head source');
    assert.equal(result.snapshot?.jobs[0]?.title, 'Head job');
    assert.deepEqual(result.snapshot?.warnings, [
      'Shared warning',
      'Head warning',
      'Later warning',
    ]);
    assert.equal(result.snapshot?.next_cursor, null);
    assert.equal(result.snapshot?.has_more, false);
  });
});
