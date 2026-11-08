import assert from 'node:assert/strict';
import test from 'node:test';

import type {
  ObservationCall,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
} from '../observation/types.ts';
import {
  filterArchivedObservationSnapshot,
  parseProjectsArchiveCutoff,
  PROJECTS_ARCHIVE_STORAGE_KEY,
  readProjectsArchiveCutoff,
  sanitizeProjectsArchiveCutoffMs,
  writeProjectsArchiveCutoff,
  type ProjectsArchiveStorage,
} from './projects-archive.ts';

const NOW_MS = Date.parse('2026-11-08T12:00:00.000Z');
const CUTOFF_MS = NOW_MS - 30 * 60 * 1000;

function makeMemoryStorage(
  initial: Record<string, string> = {}
): ProjectsArchiveStorage & { readonly entries: Map<string, string> } {
  const entries = new Map<string, string>(Object.entries(initial));
  return {
    entries,
    getItem(key: string): string | null {
      return entries.get(key) ?? null;
    },
    setItem(key: string, value: string): void {
      entries.set(key, value);
    },
  };
}

function makeSource(
  overrides: Partial<ObservationSource> = {}
): ObservationSource {
  return {
    id: 'src-1',
    label: 'Threadlane',
    project_path: '/work/threadlane',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeExecutor(
  overrides: Partial<ObservationExecutor> = {}
): ObservationExecutor {
  return {
    tool: 'agy',
    provider: 'antigravity',
    role: 'coder',
    requested_model: 'gemini-4-argon-high',
    observed_model: 'gemini-4-argon-high',
    requested_effort: 'high',
    observed_effort: 'high',
    ...overrides,
  };
}

function makeCall(overrides: Partial<ObservationCall> = {}): ObservationCall {
  return {
    id: 'call-1',
    title: 'Implement projection',
    state: 'completed',
    executor: makeExecutor(),
    started_at_ms: NOW_MS - 10_000,
    finished_at_ms: NOW_MS - 5_000,
    duration_ms: 5_000,
    usage: {
      input_tokens: 1200,
      output_tokens: 400,
      cache_read_tokens: 200,
      total_tokens: 1600,
      provenance: 'provider_receipt',
    },
    summary: 'Completed projection module.',
    ...overrides,
  };
}

function makeRequest(
  overrides: Partial<ObservationRequest> = {}
): ObservationRequest {
  return {
    source_id: 'src-1',
    id: 'req-1',
    job_id: null,
    parent_request_id: null,
    title: 'Worker handoff',
    operation: 'flow.handoff',
    project_label: 'Threadlane',
    binding_id: 'bind-1',
    thread_uuid: 'thread-1',
    thread_label: 'Projects UI',
    state: 'completed',
    started_at_ms: NOW_MS - 15_000,
    finished_at_ms: NOW_MS - 5_000,
    updated_at_ms: NOW_MS - 5_000,
    duration_ms: 10_000,
    actor: {
      label: 'Operator',
      role: 'operator',
      source: 'codex',
      model: null,
      effort: null,
      evidence: 'observed',
    },
    executor: makeExecutor(),
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: 'Dispatched worker.',
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'src-1',
    id: 'job-1',
    project_label: 'Threadlane',
    project_path: '/work/threadlane',
    title: 'Build Projects UI',
    thread_uuid: 'thread-1',
    thread_label: 'Projects UI',
    binding_id: 'bind-1',
    plan_id: 'plan-7',
    plan_revision: 3,
    state: 'completed',
    approval_status: 'approved',
    execution_status: 'completed',
    review_state: null,
    updated_at_ms: NOW_MS - 2_000,
    active_call_count: 0,
    tasks: [
      { id: 't1', title: 'Projection model', status: 'completed', depends_on: [] },
    ],
    summary: 'Implemented Projects view.',
    ...overrides,
  };
}

function makeSnapshot(
  overrides: Partial<ObservationSnapshot> = {}
): ObservationSnapshot {
  return {
    schema_version: 1,
    generated_at_ms: NOW_MS,
    sources: [makeSource()],
    jobs: [],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    ...overrides,
  };
}

test('persist, parse, and reopen version 1 archive cutoff while keeping post-cutoff work visible', () => {
  const storage = makeMemoryStorage();
  assert.equal(readProjectsArchiveCutoff(storage, NOW_MS), null);

  const writeResult = writeProjectsArchiveCutoff(storage, CUTOFF_MS, NOW_MS);
  assert.deepEqual(writeResult, {
    ok: true,
    archiveBeforeMs: CUTOFF_MS,
  });
  assert.equal(
    storage.entries.get(PROJECTS_ARCHIVE_STORAGE_KEY),
    JSON.stringify({ version: 1, archive_before_ms: CUTOFF_MS })
  );

  const reopenedCutoff = readProjectsArchiveCutoff(storage, NOW_MS + 60_000);
  assert.equal(reopenedCutoff, CUTOFF_MS);

  const snapshot = makeSnapshot({
    jobs: [
      makeJob({ id: 'job-old', updated_at_ms: CUTOFF_MS }),
      makeJob({ id: 'job-new', updated_at_ms: CUTOFF_MS + 1 }),
    ],
    requests: [
      makeRequest({
        id: 'req-old',
        job_id: 'job-old',
        started_at_ms: CUTOFF_MS - 5_000,
        finished_at_ms: CUTOFF_MS,
        updated_at_ms: CUTOFF_MS,
      }),
      makeRequest({
        id: 'req-new',
        job_id: 'job-new',
        started_at_ms: CUTOFF_MS + 1,
        finished_at_ms: CUTOFF_MS + 2_000,
        updated_at_ms: CUTOFF_MS + 2_000,
      }),
    ],
  });

  const filtered = filterArchivedObservationSnapshot(
    snapshot,
    reopenedCutoff,
    NOW_MS + 60_000
  );
  assert.deepEqual(
    filtered.jobs.map((job) => job.id),
    ['job-new']
  );
  assert.deepEqual(
    filtered.requests.map((request) => request.id),
    ['req-new']
  );
});

test('storage read/write exceptions are caught and write failure is reported cleanly', () => {
  const unavailableWrite = writeProjectsArchiveCutoff(null, CUTOFF_MS, NOW_MS);
  assert.equal(unavailableWrite.ok, false);
  if (!unavailableWrite.ok) {
    assert.match(unavailableWrite.error, /local storage is unavailable/i);
  }

  const throwingStorage: ProjectsArchiveStorage = {
    getItem(): string | null {
      throw new Error('SecurityError: storage blocked');
    },
    setItem(): void {
      throw new Error('QuotaExceededError');
    },
  };

  assert.equal(readProjectsArchiveCutoff(throwingStorage, NOW_MS), null);
  const failedWrite = writeProjectsArchiveCutoff(
    throwingStorage,
    CUTOFF_MS,
    NOW_MS
  );
  assert.equal(failedWrite.ok, false);
  if (!failedWrite.ok) {
    assert.match(failedWrite.error, /Unable to save Projects archive cutoff/i);
  }
});

test('malformed, unknown-version, nonfinite, nonpositive, unsafe-integer, and future cutoffs are rejected', () => {
  const invalidValues: unknown[] = [
    null,
    undefined,
    '',
    '1700000000000',
    0,
    -1,
    -CUTOFF_MS,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    12345.67,
    Number.MAX_SAFE_INTEGER + 1,
    NOW_MS + 1,
    NOW_MS + 3_600_000,
  ];
  for (const candidate of invalidValues) {
    assert.equal(
      sanitizeProjectsArchiveCutoffMs(candidate, NOW_MS),
      null,
      `Expected cutoff candidate ${String(candidate)} to be rejected`
    );
  }

  const invalidRawPayloads = [
    '',
    'not-json',
    'null',
    '[]',
    '12345',
    JSON.stringify({ archive_before_ms: CUTOFF_MS }),
    JSON.stringify({ version: 0, archive_before_ms: CUTOFF_MS }),
    JSON.stringify({ version: 2, archive_before_ms: CUTOFF_MS }),
    JSON.stringify({ version: '1', archive_before_ms: CUTOFF_MS }),
    JSON.stringify({ version: 1, archive_before_ms: 0 }),
    JSON.stringify({ version: 1, archive_before_ms: -100 }),
    JSON.stringify({ version: 1, archive_before_ms: 12.5 }),
    JSON.stringify({
      version: 1,
      archive_before_ms: Number.MAX_SAFE_INTEGER + 1,
    }),
    JSON.stringify({ version: 1, archive_before_ms: NOW_MS + 5_000 }),
  ];
  for (const raw of invalidRawPayloads) {
    assert.equal(parseProjectsArchiveCutoff(raw, NOW_MS), null);
  }

  const snapshot = makeSnapshot({
    jobs: [makeJob({ id: 'job-fresh', updated_at_ms: NOW_MS - 1_000 })],
    requests: [makeRequest({ id: 'req-fresh', updated_at_ms: NOW_MS - 1_000 })],
  });
  const withFutureCutoff = filterArchivedObservationSnapshot(
    snapshot,
    NOW_MS + 60_000,
    NOW_MS
  );
  assert.equal(withFutureCutoff.jobs.length, 1);
  assert.equal(withFutureCutoff.requests.length, 1);
});

test('filterArchivedObservationSnapshot never mutates the input snapshot or nested arrays', () => {
  const snapshot = makeSnapshot({
    sources: [makeSource({ id: 'src-1', message: 'Healthy' })],
    jobs: [
      makeJob({ id: 'job-old', updated_at_ms: CUTOFF_MS - 10_000 }),
      makeJob({ id: 'job-new', updated_at_ms: CUTOFF_MS + 10_000 }),
    ],
    requests: [
      makeRequest({
        id: 'req-mixed',
        job_id: 'job-new',
        updated_at_ms: CUTOFF_MS + 10_000,
        children: [
          makeCall({
            id: 'call-old',
            started_at_ms: CUTOFF_MS - 20_000,
            finished_at_ms: CUTOFF_MS - 10_000,
          }),
          makeCall({
            id: 'call-new',
            started_at_ms: CUTOFF_MS + 1_000,
            finished_at_ms: CUTOFF_MS + 5_000,
          }),
        ],
        children_count: 2,
      }),
    ],
    next_cursor: 'cursor-next',
    has_more: true,
    jobs_has_more: true,
    warnings: ['Bounded snapshot warning'],
  });
  const frozenCopy = structuredClone(snapshot);

  const filtered = filterArchivedObservationSnapshot(
    snapshot,
    CUTOFF_MS,
    NOW_MS
  );
  assert.deepEqual(snapshot, frozenCopy);
  assert.notEqual(filtered, snapshot);
  assert.notEqual(filtered.jobs, snapshot.jobs);
  assert.notEqual(filtered.requests, snapshot.requests);
  assert.notEqual(filtered.requests[0]?.children, snapshot.requests[0]?.children);
  assert.deepEqual(filtered.sources, snapshot.sources);
  assert.deepEqual(filtered.warnings, snapshot.warnings);
  assert.equal(filtered.next_cursor, 'cursor-next');
  assert.equal(filtered.has_more, true);
  assert.equal(filtered.jobs_has_more, true);
});

test('new activity in the same chat retains only its linked job and does not resurrect old jobs or requests in that chat', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-old-unrelated',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'needs_user',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 50_000,
      }),
      makeJob({
        id: 'job-old-planning',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'planning',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 40_000,
      }),
      makeJob({
        id: 'job-old-linked',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 30_000,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-old-in-same-chat',
        job_id: 'job-old-linked',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 35_000,
        finished_at_ms: CUTOFF_MS - 30_000,
        updated_at_ms: CUTOFF_MS - 30_000,
      }),
      makeRequest({
        id: 'req-fresh-in-same-chat',
        job_id: 'job-old-linked',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        started_at_ms: CUTOFF_MS + 5_000,
        finished_at_ms: CUTOFF_MS + 10_000,
        updated_at_ms: CUTOFF_MS + 10_000,
      }),
    ],
  });

  const filtered = filterArchivedObservationSnapshot(
    snapshot,
    CUTOFF_MS,
    NOW_MS
  );
  assert.deepEqual(
    filtered.jobs.map((job) => job.id),
    ['job-old-linked']
  );
  assert.deepEqual(
    filtered.requests.map((request) => request.id),
    ['req-fresh-in-same-chat']
  );
});

test('older loaded pagination pages remain hidden under an active archive cutoff while preserving pagination metadata', () => {
  const firstPage = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-recent',
        started_at_ms: CUTOFF_MS + 2_000,
        finished_at_ms: CUTOFF_MS + 4_000,
        updated_at_ms: CUTOFF_MS + 4_000,
      }),
    ],
    next_cursor: 'page-2',
    has_more: true,
    jobs_has_more: false,
  });

  const afterLoadMore: ObservationSnapshot = {
    ...firstPage,
    jobs: [
      makeJob({
        id: 'job-older-page',
        state: 'failed',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 120_000,
      }),
    ],
    requests: [
      ...firstPage.requests,
      makeRequest({
        id: 'req-older-page-1',
        job_id: 'job-older-page',
        state: 'failed',
        started_at_ms: CUTOFF_MS - 130_000,
        finished_at_ms: CUTOFF_MS - 120_000,
        updated_at_ms: CUTOFF_MS - 120_000,
      }),
      makeRequest({
        id: 'req-older-page-2',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 200_000,
        finished_at_ms: CUTOFF_MS - 190_000,
        updated_at_ms: CUTOFF_MS - 190_000,
      }),
    ],
    next_cursor: 'page-3',
    has_more: true,
  };

  const filtered = filterArchivedObservationSnapshot(
    afterLoadMore,
    CUTOFF_MS,
    NOW_MS
  );
  assert.deepEqual(filtered.jobs, []);
  assert.deepEqual(
    filtered.requests.map((request) => request.id),
    ['req-recent']
  );
  assert.equal(filtered.next_cursor, 'page-3');
  assert.equal(filtered.has_more, true);
});

test('active jobs remain visible without reviving completed requests while queued requests retain their necessary ancestors', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-running-checks',
        state: 'running',
        execution_status: 'running',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 60_000,
      }),
      makeJob({
        id: 'job-active-calls',
        state: 'ready',
        active_call_count: 1,
        updated_at_ms: CUTOFF_MS - 60_000,
      }),
      makeJob({
        id: 'job-queued-parent',
        state: 'ready',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 60_000,
      }),
      makeJob({
        id: 'job-stale-summary',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 60_000,
        summary: 'Running integration checks in background...',
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-tied-to-checks',
        job_id: 'job-running-checks',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 70_000,
        finished_at_ms: CUTOFF_MS - 60_000,
        updated_at_ms: CUTOFF_MS - 60_000,
      }),
      makeRequest({
        id: 'req-tied-to-active-calls',
        job_id: 'job-active-calls',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 70_000,
        finished_at_ms: CUTOFF_MS - 60_000,
        updated_at_ms: CUTOFF_MS - 60_000,
      }),
      makeRequest({
        id: 'req-root-parent',
        job_id: 'job-queued-parent',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 90_000,
        finished_at_ms: CUTOFF_MS - 80_000,
        updated_at_ms: CUTOFF_MS - 80_000,
      }),
      makeRequest({
        id: 'req-queued-child',
        parent_request_id: 'req-root-parent',
        state: 'queued',
        started_at_ms: CUTOFF_MS - 50_000,
        finished_at_ms: null,
        updated_at_ms: CUTOFF_MS - 50_000,
      }),
      makeRequest({
        id: 'req-stale-summary',
        job_id: 'job-stale-summary',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 70_000,
        finished_at_ms: CUTOFF_MS - 60_000,
        updated_at_ms: CUTOFF_MS - 60_000,
        summary: 'Still running worker handoff',
      }),
    ],
  });

  const filtered = filterArchivedObservationSnapshot(
    snapshot,
    CUTOFF_MS,
    NOW_MS
  );
  assert.deepEqual(
    filtered.jobs.map((job) => job.id),
    ['job-running-checks', 'job-active-calls', 'job-queued-parent']
  );
  assert.deepEqual(
    filtered.requests.map((request) => request.id),
    [
      'req-root-parent',
      'req-queued-child',
    ]
  );
});

test('recorded review waits are archived while a live reviewer retains its job context without old completed requests', () => {
  const staleReview = makeJob({
    id: 'job-stale-review',
    state: 'reviewing',
    execution_status: 'ready_for_review',
    review_state: 'pending_review',
    active_call_count: 0,
    updated_at_ms: CUTOFF_MS - 60_000,
  });
  const snapshot = makeSnapshot({ jobs: [staleReview], requests: [] });
  assert.deepEqual(
    filterArchivedObservationSnapshot(snapshot, CUTOFF_MS, NOW_MS).jobs,
    []
  );

  const withReviewer = makeSnapshot({
    jobs: [staleReview],
    requests: [
      makeRequest({
        id: 'req-old-completed',
        job_id: staleReview.id,
        state: 'completed',
        started_at_ms: CUTOFF_MS - 80_000,
        finished_at_ms: CUTOFF_MS - 70_000,
        updated_at_ms: CUTOFF_MS - 70_000,
      }),
      makeRequest({
        id: 'req-live-reviewer',
        job_id: staleReview.id,
        state: 'running',
        started_at_ms: CUTOFF_MS - 50_000,
        finished_at_ms: null,
        updated_at_ms: CUTOFF_MS - 50_000,
      }),
    ],
  });
  const filtered = filterArchivedObservationSnapshot(withReviewer, CUTOFF_MS, NOW_MS);
  assert.deepEqual(filtered.jobs.map((job) => job.id), [staleReview.id]);
  assert.deepEqual(filtered.requests.map((request) => request.id), ['req-live-reviewer']);
  assert.equal(staleReview.state, 'reviewing');
  assert.equal(withReviewer.requests.length, 2);
});

test('nested child calls filter out old completed calls while keeping running or newer sibling calls', () => {
  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-with-mixed-children',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 40_000,
        finished_at_ms: CUTOFF_MS - 10_000,
        updated_at_ms: CUTOFF_MS - 10_000,
        children: [
          makeCall({
            id: 'call-old-completed',
            state: 'completed',
            started_at_ms: CUTOFF_MS - 40_000,
            finished_at_ms: CUTOFF_MS - 30_000,
          }),
          makeCall({
            id: 'call-old-running',
            state: 'running',
            started_at_ms: CUTOFF_MS - 25_000,
            finished_at_ms: null,
          }),
          makeCall({
            id: 'call-new-completed',
            state: 'completed',
            started_at_ms: CUTOFF_MS + 1_000,
            finished_at_ms: CUTOFF_MS + 3_000,
          }),
        ],
        children_count: 3,
      }),
    ],
  });

  const filtered = filterArchivedObservationSnapshot(
    snapshot,
    CUTOFF_MS,
    NOW_MS
  );
  assert.equal(filtered.requests.length, 1);
  assert.deepEqual(
    filtered.requests[0]?.children.map((call) => call.id),
    ['call-old-running', 'call-new-completed']
  );
  assert.equal(filtered.requests[0]?.children_count, 2);
});

test('equal job and request IDs across different sources remain strictly isolated', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-a', label: 'Alpha' }),
      makeSource({ id: 'src-b', label: 'Beta' }),
    ],
    jobs: [
      makeJob({
        source_id: 'src-a',
        id: 'job-shared',
        state: 'running',
        active_call_count: 1,
        updated_at_ms: CUTOFF_MS - 20_000,
      }),
      makeJob({
        source_id: 'src-b',
        id: 'job-shared',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: CUTOFF_MS - 20_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-a',
        id: 'req-parent',
        job_id: 'job-shared',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 30_000,
        finished_at_ms: CUTOFF_MS - 20_000,
        updated_at_ms: CUTOFF_MS - 20_000,
      }),
      makeRequest({
        source_id: 'src-a',
        id: 'req-child',
        parent_request_id: 'req-parent',
        job_id: 'job-shared',
        state: 'completed',
        started_at_ms: CUTOFF_MS + 1_000,
        finished_at_ms: CUTOFF_MS + 2_000,
        updated_at_ms: CUTOFF_MS + 2_000,
      }),
      makeRequest({
        source_id: 'src-b',
        id: 'req-parent',
        job_id: 'job-shared',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 30_000,
        finished_at_ms: CUTOFF_MS - 20_000,
        updated_at_ms: CUTOFF_MS - 20_000,
      }),
      makeRequest({
        source_id: 'src-b',
        id: 'req-child',
        parent_request_id: 'req-parent',
        job_id: 'job-shared',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 15_000,
        finished_at_ms: CUTOFF_MS - 10_000,
        updated_at_ms: CUTOFF_MS - 10_000,
      }),
    ],
  });

  const filtered = filterArchivedObservationSnapshot(
    snapshot,
    CUTOFF_MS,
    NOW_MS
  );
  assert.deepEqual(
    filtered.jobs.map((job) => `${job.source_id}:${job.id}`),
    ['src-a:job-shared']
  );
  assert.deepEqual(
    filtered.requests.map((request) => `${request.source_id}:${request.id}`),
    ['src-a:req-parent', 'src-a:req-child']
  );
});

test('records with missing timestamps are conservatively retained rather than silently hidden', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-missing-ts',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: null,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-missing-ts',
        state: 'completed',
        started_at_ms: null,
        finished_at_ms: null,
        updated_at_ms: 0,
        delivery: [],
      }),
      makeRequest({
        id: 'req-with-unamped-call',
        state: 'completed',
        started_at_ms: CUTOFF_MS - 20_000,
        finished_at_ms: CUTOFF_MS - 10_000,
        updated_at_ms: CUTOFF_MS - 10_000,
        children: [
          makeCall({
            id: 'call-missing-ts',
            state: 'completed',
            started_at_ms: null,
            finished_at_ms: null,
          }),
        ],
        children_count: 1,
      }),
    ],
  });

  const filtered = filterArchivedObservationSnapshot(
    snapshot,
    CUTOFF_MS,
    NOW_MS
  );
  assert.deepEqual(
    filtered.jobs.map((job) => job.id),
    ['job-missing-ts']
  );
  assert.deepEqual(
    filtered.requests.map((request) => request.id),
    ['req-missing-ts', 'req-with-unamped-call']
  );
  assert.deepEqual(
    filtered.requests[1]?.children.map((call) => call.id),
    ['call-missing-ts']
  );
});
