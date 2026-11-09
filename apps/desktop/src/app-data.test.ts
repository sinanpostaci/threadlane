import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DesktopAppCoordinator,
  mergeHealthyObservationSources,
  mergeScopedObservationJobs,
  mergeScopedObservationRequests,
  REGISTRY_POLL_EVERY_N_TICKS,
  type DesktopCoordinatorState,
} from './app-data.ts';
import type {
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSnapshotRequest,
  ObservationSource,
} from './observation/types.ts';
import type {
  RegistrySnapshot,
  DesktopToolVersionsSnapshot,
} from './registry/types.ts';

const NOW_MS = Date.parse('2026-11-09T10:00:00.000Z');
const UUID_A = '11111111-2222-4333-8444-555555555555';
const UUID_B = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';

function makeSource(
  overrides: Partial<ObservationSource> = {}
): ObservationSource {
  return {
    id: 'src-a',
    label: 'Alpha Project',
    project_path: '/work/alpha',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'src-a',
    id: 'job-1',
    project_label: 'Alpha Project',
    project_path: '/work/alpha',
    title: 'Coordinator test job',
    thread_uuid: UUID_A,
    thread_label: 'Chat A',
    binding_id: 'bind-a',
    plan_id: 'plan-1',
    plan_revision: 1,
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS - 5_000,
    active_call_count: 1,
    tasks: [],
    summary: 'Running job',
    ...overrides,
  };
}

function makeRequest(
  overrides: Partial<ObservationRequest> = {}
): ObservationRequest {
  return {
    source_id: 'src-a',
    id: 'req-1',
    job_id: 'job-1',
    parent_request_id: null,
    title: 'Worker step',
    operation: 'flow.handoff',
    project_label: 'Alpha Project',
    binding_id: 'bind-a',
    thread_uuid: UUID_A,
    thread_label: 'Chat A',
    state: 'running',
    started_at_ms: NOW_MS - 10_000,
    finished_at_ms: null,
    updated_at_ms: NOW_MS - 2_000,
    duration_ms: null,
    actor: {
      label: 'Operator',
      role: 'operator',
      source: 'codex',
      model: null,
      effort: null,
      evidence: 'observed',
    },
    executor: {
      tool: 'agy',
      provider: 'antigravity',
      role: 'coder',
      requested_model: 'gemini-4-argon-high',
      observed_model: 'gemini-4-argon-high',
      requested_effort: 'high',
      observed_effort: 'high',
    },
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: 'Step summary',
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
    jobs: [makeJob()],
    requests: [makeRequest()],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    ...overrides,
  };
}

function makeRegistry(
  overrides: Partial<RegistrySnapshot> = {}
): RegistrySnapshot {
  return {
    revision: 1,
    tools: [
      {
        id: 'codex-cli',
        label: 'Codex CLI',
        adapter: 'codex_cli',
        executable_path: null,
        enabled: true,
      },
    ],
    models: [],
    profiles: [],
    schema_version: 1,
    default_profile_id: null,
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('app-data DesktopAppCoordinator', () => {
  it('polls only while window is visible and refreshes immediately when focus/visibility returns', async () => {
    let fetchCount = 0;
    const states: DesktopCoordinatorState[] = [];

    const coordinator = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: async () => {
          fetchCount += 1;
          return makeSnapshot({ generated_at_ms: NOW_MS + fetchCount * 1_000 });
        },
        now: () => NOW_MS,
      },
      (state) => {
        states.push(state);
      },
      { visible: false, focused: false }
    );

    assert.equal(coordinator.canPoll(), false);
    await coordinator.pollTick();
    assert.equal(fetchCount, 0);

    // Restoring visibility triggers an immediate refresh
    coordinator.setVisibility(true, true);
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(fetchCount, 1);
    assert.ok(states.at(-1)?.observationSnapshot);

    // Polling while visible executes
    await coordinator.pollTick();
    assert.equal(fetchCount, 2);
  });

  it('coalesces concurrent refresh requests into one in-flight + one bounded pending refresh', async () => {
    const inflight1 = deferred<ObservationSnapshot>();
    const inflight2 = deferred<ObservationSnapshot>();
    let callCount = 0;

    const coordinator = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: () => {
          callCount += 1;
          return callCount === 1 ? inflight1.promise : inflight2.promise;
        },
        now: () => NOW_MS,
      },
      () => {}
    );

    const p1 = coordinator.refreshObservation();
    // Fire 4 concurrent refreshes while p1 is in flight
    void coordinator.refreshObservation();
    void coordinator.refreshObservation();
    void coordinator.refreshObservation();
    void coordinator.refreshObservation();

    assert.equal(callCount, 1);

    inflight1.resolve(makeSnapshot({ generated_at_ms: NOW_MS + 1_000 }));
    await p1;
    await new Promise((r) => setTimeout(r, 10));

    // Exactly one coalesced follow-up request should have been launched
    assert.equal(callCount, 2);

    inflight2.resolve(makeSnapshot({ generated_at_ms: NOW_MS + 2_000 }));
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(
      coordinator.getState().observationSnapshot?.generated_at_ms,
      NOW_MS + 2_000
    );
  });

  it('rejects stale out-of-order observation snapshots and preserves user chat selection', async () => {
    let callIdx = 0;
    const coordinator = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: async () => {
          callIdx += 1;
          if (callIdx === 1) {
            return makeSnapshot({
              generated_at_ms: NOW_MS + 5_000,
              jobs: [
                makeJob({ id: 'job-a', thread_uuid: UUID_A, thread_label: 'Chat A' }),
                makeJob({ id: 'job-b', thread_uuid: UUID_B, thread_label: 'Chat B' }),
              ],
            });
          }
          // Second response has an older generated_at_ms (stale out-of-order snapshot)
          return makeSnapshot({
            generated_at_ms: NOW_MS + 1_000,
            jobs: [makeJob({ id: 'job-a', thread_uuid: UUID_A, thread_label: 'Stale Chat A' })],
          });
        },
        now: () => NOW_MS,
      },
      () => {}
    );

    await coordinator.refreshObservation({ mode: 'replace' });
    const proj1 = coordinator.projectWorkspace(NOW_MS);
    const chatB = proj1.projects[0]!.chats.find((c) => c.label === 'Chat B')!;
    coordinator.selectChat(chatB.key);
    assert.equal(coordinator.getState().selectedChatKey, chatB.key);

    // Stale refresh must be rejected
    await coordinator.refreshObservation({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().observationSnapshot?.generated_at_ms,
      NOW_MS + 5_000
    );
    assert.equal(coordinator.getState().selectedChatKey, chatB.key);
  });

  it('preserves healthy sources and head page data when a later pagination page fails', async () => {
    const calls: ObservationSnapshotRequest[] = [];
    const coordinator = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: async (req) => {
          calls.push(req ?? {});
          if (!req?.cursor) {
            // Head page succeeds with 60 requests and next_cursor
            return makeSnapshot({
              sources: [makeSource({ id: 'src-a', availability: 'available' })],
              jobs: [makeJob({ id: 'job-1', source_id: 'src-a' })],
              requests: Array.from({ length: 50 }, (_, i) =>
                makeRequest({ id: `req-${i}`, source_id: 'src-a' })
              ),
              has_more: true,
              next_cursor: 'page-2',
            });
          }
          throw new Error('Cursor page-2 timed out');
        },
        now: () => NOW_MS,
      },
      () => {}
    );

    // Seed with 60 requests so refresh requests a multi-page window
    await coordinator.refreshObservation({ mode: 'replace' });
    assert.equal(coordinator.getState().observationSnapshot?.requests.length, 50);

    // Now test loadMoreHistory failing on page-2: must NOT clear healthy source or existing requests
    await coordinator.loadMoreHistory();
    const stateAfterMoreFail = coordinator.getState();
    assert.ok(stateAfterMoreFail.observationSnapshot);
    assert.equal(
      stateAfterMoreFail.observationSnapshot.sources[0]!.availability,
      'available'
    );
    assert.equal(stateAfterMoreFail.observationSnapshot.requests.length, 50);
    assert.equal(stateAfterMoreFail.observationError, 'Cursor page-2 timed out');
  });

  it('keeps sources strictly scoped when merging jobs, requests, and source health across pages', () => {
    const reqs = mergeScopedObservationRequests(
      [makeRequest({ source_id: 'src-a', id: 'shared-req', title: 'From A' })],
      [makeRequest({ source_id: 'src-b', id: 'shared-req', title: 'From B' })]
    );
    assert.equal(reqs.length, 2);
    assert.equal(reqs[0]!.title, 'From A');
    assert.equal(reqs[1]!.title, 'From B');

    const jobs = mergeScopedObservationJobs(
      [makeJob({ source_id: 'src-a', id: 'shared-job', title: 'Job A' })],
      [makeJob({ source_id: 'src-b', id: 'shared-job', title: 'Job B' })]
    );
    assert.equal(jobs.length, 2);

    const sources = mergeHealthyObservationSources(
      [makeSource({ id: 'src-a', availability: 'available' })],
      [makeSource({ id: 'src-a', availability: 'unavailable', message: 'Later page glitch' })]
    );
    assert.equal(sources.length, 1);
    assert.equal(sources[0]!.availability, 'available');
  });

  it('avoids registry and tool-version rescans on observation ticks and rejects stale registry revisions', async () => {
    let obsCalls = 0;
    let regCalls = 0;
    let verCalls = 0;
    let currentReg = makeRegistry({ revision: 5 });

    const coordinator = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: async () => {
          obsCalls += 1;
          return makeSnapshot({ generated_at_ms: NOW_MS + obsCalls * 1_000 });
        },
        fetchRegistrySnapshot: async () => {
          regCalls += 1;
          return currentReg;
        },
        fetchToolVersions: async (): Promise<DesktopToolVersionsSnapshot> => {
          verCalls += 1;
          return {
            registry_revision: currentReg.revision,
            tools: [],
          };
        },
        now: () => NOW_MS,
      },
      () => {}
    );

    await coordinator.initialize();
    await new Promise((r) => setTimeout(r, 10));

    assert.equal(obsCalls, 1);
    assert.equal(regCalls, 1);
    // Startup tool-version scan runs exactly once
    assert.equal(verCalls, 1);
    assert.equal(coordinator.getState().toolVersions.status, 'ready');
    assert.equal(coordinator.getState().toolVersions.inspectedRegistryRevision, 5);

    // Run 5 observation poll ticks (< REGISTRY_POLL_EVERY_N_TICKS)
    for (let i = 0; i < 5; i += 1) {
      await coordinator.pollTick();
    }
    assert.equal(obsCalls, 6);
    assert.equal(regCalls, 1);
    assert.equal(verCalls, 1);

    // Advance to REGISTRY_POLL_EVERY_N_TICKS: registry refreshes once, but tool versions are NOT rescanned
    for (let i = 5; i < REGISTRY_POLL_EVERY_N_TICKS; i += 1) {
      await coordinator.pollTick();
    }
    assert.equal(regCalls, 2);
    assert.equal(verCalls, 1);

    // Reject stale external registry revision (revision 3 < 5)
    const appliedStale = coordinator.applyExternalRegistrySnapshot(
      makeRegistry({ revision: 3 })
    );
    assert.equal(appliedStale, false);
    assert.equal(coordinator.getState().registry?.revision, 5);

    // Accept newer external registry revision with modified tool executable path -> triggers version recheck
    currentReg = makeRegistry({
      revision: 8,
      tools: [
        {
          id: 'codex-cli',
          label: 'Codex CLI',
          adapter: 'codex_cli',
          executable_path: '/custom/bin/codex',
          enabled: true,
        },
      ],
    });
    const appliedFresh = coordinator.applyExternalRegistrySnapshot(currentReg);
    assert.equal(appliedFresh, true);
    assert.equal(coordinator.getState().registry?.revision, 8);
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(verCalls, 2);
    assert.equal(coordinator.getState().toolVersions.status, 'ready');
    assert.equal(coordinator.getState().toolVersions.inspectedRegistryRevision, 8);
  });

  it('settles a mismatched version response and recovers on an explicit recheck', async () => {
    let responseRevision = 3;
    let versionCalls = 0;
    const coordinator = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: async () => makeSnapshot(),
        fetchRegistrySnapshot: async () => makeRegistry({ revision: 5 }),
        fetchToolVersions: async () => {
          versionCalls += 1;
          return { registry_revision: responseRevision, tools: [] };
        },
        now: () => NOW_MS,
      },
      () => {}
    );
    await coordinator.initialize();
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(coordinator.getState().toolVersions.status, 'stale');
    assert.equal(coordinator.getState().toolVersions.activeTicketId, null);
    assert.equal(versionCalls, 1);
    responseRevision = 5;
    coordinator.recheckToolVersions();
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(coordinator.getState().toolVersions.status, 'ready');
    assert.equal(coordinator.getState().toolVersions.inspectedRegistryRevision, 5);
    assert.equal(versionCalls, 2);
    coordinator.dispose();
  });

  it('validates canonical CodexChatTarget and reports IPC failures quietly', async () => {
    const targetsSent: unknown[] = [];
    let shouldFail = false;

    const coordinator = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: async () => makeSnapshot(),
        openCodexChat: async (target) => {
          targetsSent.push(target);
          if (shouldFail) {
            throw new Error('Codex binary not running');
          }
        },
      },
      () => {}
    );

    // Reject invalid non-UUID target without calling bridge
    const badResult = await coordinator.openCodexChat({
      source_id: 'src-a',
      thread_uuid: 'not-a-valid-uuid',
      job_id: 'job-1',
    });
    assert.equal(badResult, false);
    assert.equal(targetsSent.length, 0);
    assert.ok(coordinator.getState().codexOpenError);

    // Valid canonical target succeeds
    const okResult = await coordinator.openCodexChat({
      source_id: 'src-a',
      thread_uuid: UUID_A,
      job_id: 'job-1',
    });
    assert.equal(okResult, true);
    assert.equal(targetsSent.length, 1);
    assert.deepEqual(targetsSent[0], {
      source_id: 'src-a',
      thread_uuid: UUID_A,
      job_id: 'job-1',
    });
    assert.equal(coordinator.getState().codexOpenError, null);

    // IPC failure reported quietly in state
    shouldFail = true;
    const failResult = await coordinator.openCodexChat({
      source_id: 'src-a',
      thread_uuid: UUID_A,
      job_id: null,
    });
    assert.equal(failResult, false);
    assert.equal(
      coordinator.getState().codexOpenError,
      'Codex binary not running'
    );
  });
});
