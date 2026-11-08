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
import { projectProjects } from './projects-model.ts';

const NOW_MS = Date.parse('2026-11-08T12:00:00.000Z');
const ONE_HOUR_MS = 60 * 60 * 1000;

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

function makeLocalMcpExecutor(): ObservationExecutor {
  return {
    tool: 'threadlane_mcp',
    provider: 'local',
    role: null,
    requested_model: null,
    observed_model: null,
    requested_effort: null,
    observed_effort: null,
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
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS - 2_000,
    active_call_count: 1,
    tasks: [
      { id: 't1', title: 'Projection model', status: 'completed', depends_on: [] },
      { id: 't2', title: 'Page component', status: 'running', depends_on: ['t1'] },
    ],
    summary: 'Implementing Projects view.',
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

test('same-name different chats and same-name different projects stay isolated', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-a', label: 'App', project_path: '/orgs/a/app' }),
      makeSource({ id: 'src-b', label: 'App', project_path: '/orgs/b/app' }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-a',
        id: 'req-a1',
        project_label: 'App',
        thread_uuid: 'thread-a1',
        binding_id: 'bind-a1',
        thread_label: 'Bug Triage',
      }),
      makeRequest({
        source_id: 'src-a',
        id: 'req-a2',
        project_label: 'App',
        thread_uuid: 'thread-a2',
        binding_id: 'bind-a2',
        thread_label: 'Bug Triage',
      }),
      makeRequest({
        source_id: 'src-b',
        id: 'req-b1',
        project_label: 'App',
        thread_uuid: 'thread-b1',
        binding_id: 'bind-b1',
        thread_label: 'Bug Triage',
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 2);
  const firstProject = projection.projects[0];
  const secondProject = projection.projects[1];
  assert.ok(firstProject);
  assert.ok(secondProject);
  assert.notEqual(firstProject.key, secondProject.key);
  assert.equal(firstProject.path, '/orgs/a/app');
  assert.equal(secondProject.path, '/orgs/b/app');
  assert.equal(firstProject.sessions.length, 2);
  assert.equal(firstProject.sessions[0]?.label, 'Bug Triage');
  assert.equal(firstProject.sessions[1]?.label, 'Bug Triage');
  assert.notEqual(firstProject.sessions[0]?.key, firstProject.sessions[1]?.key);
  assert.equal(firstProject.sessions[0]?.threadUuid, 'thread-a1');
  assert.equal(firstProject.sessions[1]?.threadUuid, 'thread-a2');
});

test('same IDs across different sources do not collide in projects, sessions, jobs, or scope', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-1', label: 'Service', project_path: null }),
      makeSource({ id: 'src-2', label: 'Service', project_path: null }),
    ],
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-shared',
        project_label: 'Service',
        project_path: null,
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 1 Job',
      }),
      makeJob({
        source_id: 'src-2',
        id: 'job-shared',
        project_label: 'Service',
        project_path: null,
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 2 Job',
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-shared',
        job_id: 'job-shared',
        project_label: 'Service',
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 1 Call',
      }),
      makeRequest({
        source_id: 'src-2',
        id: 'req-shared',
        job_id: 'job-shared',
        project_label: 'Service',
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 2 Call',
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 2);
  assert.notEqual(projection.projects[0]?.key, projection.projects[1]?.key);
  assert.notEqual(
    projection.projects[0]?.sessions[0]?.key,
    projection.projects[1]?.sessions[0]?.key
  );
  assert.notEqual(
    projection.projects[0]?.sessions[0]?.jobs[0]?.key,
    projection.projects[1]?.sessions[0]?.jobs[0]?.key
  );

  const scoped = projectProjects(snapshot, {
    nowMs: NOW_MS,
    scope: { source_id: 'src-2', job_id: 'job-shared' },
  });
  assert.equal(scoped.projects.length, 1);
  assert.equal(scoped.projects[0]?.sessions.length, 1);
  assert.equal(scoped.projects[0]?.sessions[0]?.jobs[0]?.job.title, 'Source 2 Job');
  assert.equal(scoped.projects[0]?.sessions[0]?.calls[0]?.title, 'Source 2 Call');
});

test('missing thread and binding identities isolate to job or request without collapsing by label', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-a',
        title: 'Job Alpha',
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
      }),
      makeJob({
        id: 'job-b',
        title: 'Job Beta',
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-linked-a',
        job_id: 'job-a',
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
        title: 'Call for Job Alpha',
      }),
      makeRequest({
        id: 'req-unlinked-1',
        job_id: null,
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
        title: 'Standalone Call 1',
      }),
      makeRequest({
        id: 'req-unlinked-2',
        job_id: null,
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
        title: 'Standalone Call 2',
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 1);
  const sessions = projection.projects[0]?.sessions ?? [];
  assert.equal(sessions.length, 4);

  const jobASession = sessions.find((s) => s.jobs[0]?.job.id === 'job-a');
  assert.ok(jobASession);
  assert.equal(jobASession.calls.length, 1);
  assert.equal(jobASession.calls[0]?.title, 'Call for Job Alpha');
});

test('duplicate requests and jobs deduplicate stably by source+id and newest meaningful timestamp', () => {
  const olderRequest = makeRequest({
    id: 'req-dup',
    state: 'running',
    updated_at_ms: NOW_MS - 20_000,
    finished_at_ms: null,
    title: 'Stale running copy',
  });
  const newerRequest = makeRequest({
    id: 'req-dup',
    state: 'completed',
    updated_at_ms: NOW_MS - 5_000,
    finished_at_ms: NOW_MS - 5_000,
    title: 'Newest completed copy',
  });
  const olderJob = makeJob({
    id: 'job-dup',
    state: 'planning',
    updated_at_ms: NOW_MS - 30_000,
  });
  const newerJob = makeJob({
    id: 'job-dup',
    state: 'reviewing',
    updated_at_ms: NOW_MS - 1_000,
  });

  const forward = projectProjects(
    makeSnapshot({
      jobs: [olderJob, newerJob],
      requests: [olderRequest, newerRequest],
    }),
    { nowMs: NOW_MS }
  );
  const reverse = projectProjects(
    makeSnapshot({
      jobs: [newerJob, olderJob],
      requests: [newerRequest, olderRequest],
    }),
    { nowMs: NOW_MS }
  );

  const forwardSession = forward.projects[0]?.sessions[0];
  const reverseSession = reverse.projects[0]?.sessions[0];
  assert.ok(forwardSession);
  assert.ok(reverseSession);
  assert.equal(forwardSession.calls.length, 1);
  assert.equal(forwardSession.calls[0]?.title, 'Newest completed copy');
  assert.equal(forwardSession.calls[0]?.running, false);
  assert.equal(forwardSession.jobs.length, 1);
  assert.equal(forwardSession.jobs[0]?.stateLabel, 'Awaiting review');
  assert.deepEqual(forwardSession, reverseSession);
});

test('root completed with running child deduplicates root/child identity and displays running child while keeping local MCP in recent', () => {
  const runningChild = makeCall({
    id: 'call-worker-1',
    title: 'Argon implementation worker',
    state: 'running',
    started_at_ms: NOW_MS - 8_000,
    finished_at_ms: null,
    duration_ms: null,
  });
  const queuedChild = makeCall({
    id: 'call-worker-2',
    title: 'Queued follow-up',
    state: 'queued',
    started_at_ms: null,
    finished_at_ms: null,
    duration_ms: null,
  });

  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-root',
        title: 'Dispatch worker',
        state: 'completed',
        executor: makeExecutor(),
        children: [runningChild, queuedChild],
        children_count: 2,
      }),
      makeRequest({
        id: 'call-worker-1',
        parent_request_id: 'req-root',
        title: 'Argon implementation worker',
        state: 'running',
        started_at_ms: NOW_MS - 8_000,
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 2_000,
        executor: makeExecutor(),
      }),
      makeRequest({
        id: 'req-mcp-status',
        title: 'Check threadlane status',
        operation: 'threadlane.status',
        state: 'running',
        executor: makeLocalMcpExecutor(),
        children: [],
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.calls.length, 2);
  assert.equal(session.runningCount, 1);
  assert.equal(projection.runningCount, 1);
  assert.equal(session.calls[0]?.title, 'Argon implementation worker');
  assert.equal(session.calls[0]?.running, true);
  assert.equal(session.calls[0]?.stateLabel, 'Running');
  assert.equal(session.calls[1]?.title, 'Queued follow-up');
  assert.equal(session.calls[1]?.running, false);
  assert.equal(session.recent.length, 1);
  assert.equal(session.recent[0]?.label, 'Check threadlane status');
});

test('old active, waiting, unresolved approval, and unknown sessions are retained beyond 24h while old terminal completed/failed sessions are filtered', () => {
  const oldMs = NOW_MS - 48 * ONE_HOUR_MS;
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-old-reviewing',
        thread_uuid: 'thread-reviewing',
        thread_label: 'Active Reviewing',
        state: 'reviewing',
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-ready',
        thread_uuid: 'thread-ready',
        thread_label: 'Unresolved Approval',
        state: 'ready',
        approval_status: 'pending',
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-unknown',
        thread_uuid: 'thread-unknown',
        thread_label: 'Unknown Outcome',
        state: 'unknown',
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-done',
        thread_uuid: 'thread-done',
        thread_label: 'Old Completed',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-failed',
        thread_uuid: 'thread-failed',
        thread_label: 'Old Failed',
        state: 'failed',
        active_call_count: 0,
        updated_at_ms: oldMs,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-worker-done-under-review',
        job_id: 'job-old-reviewing',
        thread_uuid: 'thread-reviewing',
        thread_label: 'Active Reviewing',
        state: 'completed',
        updated_at_ms: oldMs,
        finished_at_ms: oldMs,
        started_at_ms: oldMs - 5_000,
      }),
      makeRequest({
        id: 'req-old-waiting',
        thread_uuid: 'thread-waiting',
        thread_label: 'Waiting For User',
        state: 'waiting_for_user',
        updated_at_ms: oldMs,
        finished_at_ms: null,
        started_at_ms: oldMs,
      }),
      makeRequest({
        id: 'req-old-done',
        job_id: 'job-old-done',
        thread_uuid: 'thread-done',
        thread_label: 'Old Completed',
        state: 'completed',
        updated_at_ms: oldMs,
        finished_at_ms: oldMs,
        started_at_ms: oldMs - 5_000,
      }),
      makeRequest({
        id: 'req-old-failed',
        job_id: 'job-old-failed',
        thread_uuid: 'thread-failed',
        thread_label: 'Old Failed',
        state: 'failed',
        updated_at_ms: oldMs,
        finished_at_ms: oldMs,
        started_at_ms: oldMs - 5_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
  });
  assert.equal(projection.hiddenSessionCount, 2);
  const sessions = projection.projects[0]?.sessions ?? [];
  assert.equal(
    sessions.map((s) => s.label).join(','),
    'Active Reviewing,Unknown Outcome,Unresolved Approval,Waiting For User'
  );

  const reviewingSession = sessions.find((s) => s.label === 'Active Reviewing');
  assert.ok(reviewingSession);
  assert.equal(reviewingSession.jobs[0]?.stateLabel, 'Awaiting review');
  assert.match(reviewingSession.stateLabel, /^(?:Older work a|A)waiting review$/);
});

test('exact 6h boundary retains session at cutoff and filters session 1ms older under 6h window', () => {
  const exactSixHoursAgo = NOW_MS - 6 * ONE_HOUR_MS;
  const beyondSixHoursAgo = exactSixHoursAgo - 1;

  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-exact-6h',
        thread_uuid: 'thread-exact',
        thread_label: 'Exact Boundary',
        state: 'completed',
        started_at_ms: exactSixHoursAgo,
        finished_at_ms: exactSixHoursAgo,
        updated_at_ms: exactSixHoursAgo,
      }),
      makeRequest({
        id: 'req-beyond-6h',
        thread_uuid: 'thread-beyond',
        thread_label: 'Beyond Boundary',
        state: 'completed',
        started_at_ms: beyondSixHoursAgo,
        finished_at_ms: beyondSixHoursAgo,
        updated_at_ms: beyondSixHoursAgo,
      }),
    ],
  });

  const sixHourView = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
  });
  assert.equal(sixHourView.hiddenSessionCount, 1);
  assert.equal(sixHourView.projects[0]?.sessions.length, 1);
  assert.equal(sixHourView.projects[0]?.sessions[0]?.label, 'Exact Boundary');

  const twentyFourHourView = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 24,
  });
  assert.equal(twentyFourHourView.hiddenSessionCount, 0);
  assert.equal(twentyFourHourView.projects[0]?.sessions.length, 2);
});

test('archive cutoff hides old inactive sessions and calls while keeping new activity in the same chat visible', () => {
  const archiveCutoffMs = NOW_MS - 30 * 60 * 1000;
  const beforeCutoffMs = archiveCutoffMs - 10_000;
  const afterCutoffMs = archiveCutoffMs + 10_000;

  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-old-hold',
        thread_uuid: 'thread-old-hold',
        thread_label: 'Old Hold Session',
        binding_id: 'bind-old-hold',
        state: 'needs_user',
        active_call_count: 0,
        updated_at_ms: beforeCutoffMs,
      }),
      makeJob({
        id: 'job-same-chat-old',
        thread_uuid: 'thread-active-chat',
        thread_label: 'Same Chat',
        binding_id: 'bind-active-chat',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: beforeCutoffMs,
      }),
      makeJob({
        id: 'job-same-chat-linked',
        thread_uuid: 'thread-active-chat',
        thread_label: 'Same Chat',
        binding_id: 'bind-active-chat',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: beforeCutoffMs,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-old-chat',
        job_id: 'job-same-chat-old',
        thread_uuid: 'thread-active-chat',
        thread_label: 'Same Chat',
        binding_id: 'bind-active-chat',
        state: 'completed',
        started_at_ms: beforeCutoffMs - 5_000,
        finished_at_ms: beforeCutoffMs,
        updated_at_ms: beforeCutoffMs,
      }),
      makeRequest({
        id: 'req-new-chat',
        job_id: 'job-same-chat-linked',
        thread_uuid: 'thread-active-chat',
        thread_label: 'Same Chat',
        binding_id: 'bind-active-chat',
        state: 'completed',
        started_at_ms: afterCutoffMs - 5_000,
        finished_at_ms: afterCutoffMs,
        updated_at_ms: afterCutoffMs,
        children: [
          makeCall({
            id: 'call-old-done',
            title: 'Archived call',
            state: 'completed',
            started_at_ms: beforeCutoffMs - 4_000,
            finished_at_ms: beforeCutoffMs,
          }),
          makeCall({
            id: 'call-fresh-done',
            title: 'Fresh call',
            state: 'completed',
            started_at_ms: afterCutoffMs - 2_000,
            finished_at_ms: afterCutoffMs,
          }),
        ],
        children_count: 2,
      }),
    ],
  });

  const beforeArchive = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
  });
  assert.deepEqual(
    beforeArchive.projects[0]?.sessions.map((s) => s.label),
    ['Old Hold Session', 'Same Chat']
  );
  const sameChatBefore = beforeArchive.projects[0]?.sessions.find(
    (s) => s.label === 'Same Chat'
  );
  assert.equal(sameChatBefore?.jobs.length, 2);
  assert.deepEqual(
    sameChatBefore?.calls.map((call) => call.title).sort(),
    ['Archived call', 'Fresh call', 'Worker handoff']
  );

  const afterArchive = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
    archiveBeforeMs: archiveCutoffMs,
  });
  assert.deepEqual(
    afterArchive.projects[0]?.sessions.map((s) => s.label),
    ['Same Chat']
  );
  const sameChatAfter = afterArchive.projects[0]?.sessions[0];
  assert.ok(sameChatAfter);
  assert.deepEqual(
    sameChatAfter.jobs.map((j) => j.job.id),
    ['job-same-chat-linked']
  );
  assert.deepEqual(
    sameChatAfter.calls.map((c) => c.title),
    ['Fresh call']
  );
});

test('archive decisions use the canonical job and request rather than stale active duplicates in either order', () => {
  const archiveBeforeMs = NOW_MS - 30 * 60 * 1000;
  const olderMs = archiveBeforeMs - 20_000;
  const newerMs = archiveBeforeMs - 10_000;
  const staleJob = makeJob({
    id: 'job-duplicate', state: 'running', execution_status: 'running',
    active_call_count: 1, updated_at_ms: olderMs,
  });
  const canonicalJob = makeJob({
    id: 'job-duplicate', state: 'reviewing', execution_status: 'ready_for_review',
    active_call_count: 0, updated_at_ms: newerMs,
  });
  const staleRequest = makeRequest({
    id: 'request-duplicate', state: 'running',
    started_at_ms: olderMs, finished_at_ms: null, updated_at_ms: olderMs,
  });
  const canonicalRequest = makeRequest({
    id: 'request-duplicate', state: 'completed',
    started_at_ms: olderMs, finished_at_ms: newerMs, updated_at_ms: newerMs,
  });
  for (const reverse of [false, true]) {
    for (const kind of ['job', 'request'] as const) {
      const jobs = kind === 'job' ? [staleJob, canonicalJob] : [];
      const requests = kind === 'request' ? [staleRequest, canonicalRequest] : [];
      const snapshot = makeSnapshot({
        jobs: reverse ? jobs.reverse() : jobs,
        requests: reverse ? requests.reverse() : requests,
      });
      const before = JSON.stringify(snapshot);
      assert.deepEqual(
        projectProjects(snapshot, { nowMs: NOW_MS, archiveBeforeMs }).projects,
        [],
        `The canonical inactive ${kind} archives regardless of duplicate order`
      );
      assert.equal(JSON.stringify(snapshot), before);
    }
  }
});

test('archive cutoff retains running checks and active calls while explicit job scope bypasses archive filter', () => {
  const archiveCutoffMs = NOW_MS - 10 * 60 * 1000;
  const oldMs = archiveCutoffMs - 30_000;

  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-reviewing',
        thread_uuid: 'thread-review',
        thread_label: 'Reviewing Checks',
        binding_id: 'bind-review',
        state: 'running',
        execution_status: 'running',
        active_call_count: 0,
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-archived-done',
        thread_uuid: 'thread-done',
        thread_label: 'Archived Completed',
        binding_id: 'bind-done',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: oldMs,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-reviewing',
        job_id: 'job-reviewing',
        thread_uuid: 'thread-review',
        thread_label: 'Reviewing Checks',
        binding_id: 'bind-review',
        state: 'completed',
        started_at_ms: oldMs - 5_000,
        finished_at_ms: oldMs,
        updated_at_ms: oldMs,
      }),
      makeRequest({
        id: 'req-running-call',
        thread_uuid: 'thread-running',
        thread_label: 'Running Worker',
        binding_id: 'bind-running',
        state: 'completed',
        started_at_ms: oldMs - 5_000,
        finished_at_ms: oldMs,
        updated_at_ms: oldMs,
        children: [
          makeCall({
            id: 'call-still-running',
            title: 'Active execution',
            state: 'running',
            started_at_ms: oldMs,
            finished_at_ms: null,
          }),
        ],
        children_count: 1,
      }),
      makeRequest({
        id: 'req-archived-done',
        job_id: 'job-archived-done',
        thread_uuid: 'thread-done',
        thread_label: 'Archived Completed',
        binding_id: 'bind-done',
        state: 'completed',
        started_at_ms: oldMs - 5_000,
        finished_at_ms: oldMs,
        updated_at_ms: oldMs,
      }),
    ],
  });

  const archivedView = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
    archiveBeforeMs: archiveCutoffMs,
  });
  assert.equal(archivedView.runningCount, 1);
  assert.deepEqual(
    archivedView.projects[0]?.sessions.map((s) => s.label),
    ['Reviewing Checks', 'Running Worker']
  );

  const scopedView = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
    archiveBeforeMs: archiveCutoffMs,
    scope: { source_id: 'src-1', job_id: 'job-archived-done' },
  });
  assert.deepEqual(
    scopedView.projects[0]?.sessions.map((s) => s.label),
    ['Archived Completed']
  );
});

test('project and session order stays alphabetical and stable after new activity and with future or nonfinite timestamps', () => {
  const initial = makeSnapshot({
    sources: [
      makeSource({ id: 'src-z', label: 'Zeta', project_path: '/work/zeta' }),
      makeSource({ id: 'src-a', label: 'Alpha', project_path: '/work/alpha' }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-z',
        id: 'req-z',
        project_label: 'Zeta',
        thread_uuid: 'thread-z',
        thread_label: 'Zeta Chat',
        updated_at_ms: NOW_MS - 1_000,
      }),
      makeRequest({
        source_id: 'src-a',
        id: 'req-a-beta',
        project_label: 'Alpha',
        thread_uuid: 'thread-beta',
        thread_label: 'Beta Session',
        updated_at_ms: NOW_MS - 1_000,
      }),
      makeRequest({
        source_id: 'src-a',
        id: 'req-a-alpha',
        project_label: 'Alpha',
        thread_uuid: 'thread-alpha',
        thread_label: 'Alpha Session',
        updated_at_ms: NOW_MS - 60_000,
      }),
    ],
  });

  const firstProjection = projectProjects(initial, { nowMs: NOW_MS });
  assert.deepEqual(
    firstProjection.projects.map((p) => p.label),
    ['Alpha', 'Zeta']
  );
  assert.deepEqual(
    firstProjection.projects[0]?.sessions.map((s) => s.label),
    ['Alpha Session', 'Beta Session']
  );

  const afterNewActivity = makeSnapshot({
    ...initial,
    requests: [
      ...initial.requests,
      makeRequest({
        source_id: 'src-a',
        id: 'req-a-beta-new',
        project_label: 'Alpha',
        thread_uuid: 'thread-beta',
        thread_label: 'Beta Session',
        state: 'running',
        started_at_ms: NOW_MS + 30_000,
        finished_at_ms: Number.NaN,
        updated_at_ms: NOW_MS + 30_000,
      }),
    ],
  });

  const secondProjection = projectProjects(afterNewActivity, { nowMs: NOW_MS });
  assert.deepEqual(
    secondProjection.projects.map((p) => p.label),
    ['Alpha', 'Zeta']
  );
  assert.deepEqual(
    secondProjection.projects[0]?.sessions.map((s) => s.label),
    ['Alpha Session', 'Beta Session']
  );
});

test('job-only snapshot projects sessions and inline job summaries with zero requests', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-only-1',
        title: 'Standalone Job Plan',
        thread_uuid: 'thread-job-only',
        thread_label: 'Planning Chat',
        state: 'planning',
        plan_id: 'plan-42',
        plan_revision: 2,
        updated_at_ms: NOW_MS - 10_000,
      }),
    ],
    requests: [],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 1);
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.label, 'Planning Chat');
  assert.equal(session.calls.length, 0);
  assert.equal(session.recent.length, 0);
  assert.equal(session.jobs.length, 1);
  assert.equal(session.jobs[0]?.stateLabel, 'Planning recorded');
  assert.equal(session.jobs[0]?.planLabel, 'Plan · revision 2');
  assert.equal(session.jobs[0]?.job.plan_id, 'plan-42');
  assert.equal(session.jobs[0]?.taskSummary, '1 running · 1 done');
});

test('missing usage stays Usage unknown, requested model is not faked as observed, and parent+child tokens are never summed', () => {
  const childWithoutObservedOrUsage = makeCall({
    id: 'call-unobserved',
    title: 'Draft patch',
    executor: makeExecutor({
      requested_model: 'opencode-go/deepseek-v4.1-flash',
      observed_model: null,
      requested_effort: 'max',
      observed_effort: null,
      tool: 'opencode-go',
      provider: 'opencode-go',
    }),
    usage: null,
  });

  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-parent-tokens',
        usage: {
          input_tokens: 500,
          output_tokens: 500,
          cache_read_tokens: 0,
          total_tokens: 1000,
          provenance: 'parent_estimate',
        },
        children: [childWithoutObservedOrUsage],
        children_count: 1,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const call = projection.projects[0]?.sessions[0]?.calls[0];
  assert.ok(call);
  assert.equal(call.usageLabel, 'Usage unknown');
  assert.equal(call.model, 'DeepSeek');
  assert.equal(call.effort, 'max');
  assert.equal(call.brand, 'opencode');
  assert.match(
    call.detail,
    /Model: requested opencode-go\/deepseek-v4\.1-flash · observed Not reported\./
  );
  assert.match(call.detail, /Effort: requested max · observed Not reported\./);
  assert.doesNotMatch(call.detail, /1,000/);
});

test('partial source coverage and bounded snapshot flags surface unavailable counts and warnings', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-ok', label: 'Primary', availability: 'available' }),
      makeSource({
        id: 'src-down',
        label: 'Secondary',
        availability: 'unavailable',
        message: 'Socket disconnected',
      }),
    ],
    has_more: true,
    jobs_has_more: true,
    warnings: ['Bounded observation page returned.'],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.unavailableSourceCount, 1);
  assert.equal(projection.hasMore, true);
  assert.equal(projection.jobsHasMore, true);
  assert.ok(
    projection.warnings.some((w) => w.includes('Bounded observation page returned.'))
  );
  assert.ok(
    projection.warnings.some((w) =>
      w.includes('Source Secondary is unavailable: Socket disconnected')
    )
  );
  assert.ok(projection.warnings.some((w) => w.includes('jobs_has_more')));
  assert.ok(projection.warnings.some((w) => w.includes('has_more')));
  assert.ok(projection.emptyMessage !== null);
});

test('completed job settles earlier failed request for summary while calls retain failed history', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-1',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        execution_status: 'completed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-failed',
        job_id: 'job-1',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        started_at_ms: NOW_MS - 20_000,
        finished_at_ms: NOW_MS - 10_000,
        updated_at_ms: NOW_MS - 10_000,
        children: [
          makeCall({
            id: 'call-failed',
            state: 'failed',
            started_at_ms: NOW_MS - 19_000,
            finished_at_ms: NOW_MS - 10_000,
          }),
        ],
        children_count: 1,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.stateLabel, 'Completed');
  assert.equal(session.attention, false);
  assert.ok(session.calls.some((call) => call.stateLabel === 'Failed'));
});

test('same chat old terminal failed job does not override current completed job or appear inline', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-old-failed',
        title: 'Old failed job',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        execution_status: 'failed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
      }),
      makeJob({
        source_id: 'src-1',
        id: 'job-current-ok',
        title: 'Current completed job',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        execution_status: 'completed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.stateLabel, 'Completed');
  assert.equal(session.attention, false);
  assert.deepEqual(
    session.jobs.map((job) => job.job.id),
    ['job-current-ok']
  );
});

test('current failed job with running call surfaces running count and needs attention', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        execution_status: 'failed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-running',
        job_id: 'job-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'running',
        started_at_ms: NOW_MS - 3_000,
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        children: [
          makeCall({
            id: 'call-running',
            state: 'running',
            started_at_ms: NOW_MS - 3_000,
            finished_at_ms: null,
          }),
        ],
        children_count: 1,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.attention, true);
  assert.equal(session.runningCount, 1);
  assert.equal(session.tone, 'danger');
  assert.ok(session.stateLabel.includes('Needs attention'));
});

test('old failed job and old failed request do not mark current completed chat failed, while current failed request still demands attention', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-old-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        execution_status: 'failed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
      }),
      makeJob({
        source_id: 'src-1',
        id: 'job-current-ok',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        execution_status: 'completed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-old-failed',
        job_id: 'job-old-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        started_at_ms: NOW_MS - 48 * ONE_HOUR_MS - 5_000,
        finished_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
        updated_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
        children: [
          makeCall({
            id: 'call-old-failed',
            state: 'failed',
            started_at_ms: NOW_MS - 48 * ONE_HOUR_MS - 4_000,
            finished_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
          }),
        ],
        children_count: 1,
      }),
      makeRequest({
        source_id: 'src-1',
        id: 'req-current-failed',
        job_id: null,
        thread_uuid: 'thread-2',
        thread_label: 'Active Failing Chat',
        binding_id: 'bind-2',
        state: 'failed',
        started_at_ms: NOW_MS - 4_000,
        finished_at_ms: NOW_MS - 1_000,
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const settledSession = projection.projects[0]?.sessions.find(
    (item) => item.label === 'Projects UI'
  );
  assert.ok(settledSession);
  assert.equal(settledSession.stateLabel, 'Completed');
  assert.equal(settledSession.attention, false);
  assert.equal(settledSession.calls.length, 0);

  const failingSession = projection.projects[0]?.sessions.find(
    (item) => item.label === 'Active Failing Chat'
  );
  assert.ok(failingSession);
  assert.equal(failingSession.stateLabel, 'Failed');
  assert.equal(failingSession.attention, true);
});

test('child calls and local child events prefer own timestamps over newer parent updated_at_ms', () => {
  const oldFinishedMs = NOW_MS - 48 * ONE_HOUR_MS;
  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-parent-updated',
        state: 'completed',
        started_at_ms: oldFinishedMs - 5_000,
        finished_at_ms: NOW_MS - 1_000,
        updated_at_ms: NOW_MS - 1_000,
        children: [
          makeCall({
            id: 'call-old-child',
            title: 'Old finished child',
            state: 'failed',
            started_at_ms: oldFinishedMs - 2_000,
            finished_at_ms: oldFinishedMs,
          }),
          makeCall({
            id: 'call-new-child',
            title: 'Fresh finished child',
            state: 'completed',
            started_at_ms: NOW_MS - 3_000,
            finished_at_ms: NOW_MS - 1_000,
          }),
          makeCall({
            id: 'call-old-local',
            title: 'Old local child',
            state: 'completed',
            executor: makeLocalMcpExecutor(),
            started_at_ms: oldFinishedMs - 1_000,
            finished_at_ms: oldFinishedMs,
          }),
        ],
        children_count: 3,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.deepEqual(
    session.calls.map((call) => call.title),
    ['Fresh finished child']
  );
  const localEvent = session.recent.find((event) => event.label === 'Old local child');
  assert.ok(localEvent);
  assert.equal(localEvent.atMs, oldFinishedMs);
});

test('multiple old pending approvals with recent local MCP stay older and reason-specific', () => {
  const nov4Ms = NOW_MS - 96 * ONE_HOUR_MS;
  const nov6Ms = NOW_MS - 48 * ONE_HOUR_MS;
  const aliases = [
    'awaiting_approval',
    'pending_approval',
    'approval_needed',
    'needs_approval',
    'waiting_for_approval',
  ];
  const snapshot = makeSnapshot({
    jobs: aliases.map((alias, idx) =>
      makeJob({
        id: `job-old-plan-${idx + 1}`,
        title: `Legacy plan ${idx + 1} (${alias})`,
        plan_id: `plan-${idx + 1}`,
        plan_revision: 1,
        state: idx % 2 === 0 ? 'needs_user' : 'ready',
        approval_status: alias,
        execution_status: 'pending',
        active_call_count: 0,
        updated_at_ms: idx < 3 ? nov4Ms + idx * 1_000 : nov6Ms + idx * 1_000,
      })
    ),
    requests: [
      makeRequest({
        id: 'req-mcp-status',
        title: 'Status refresh',
        operation: 'mcp.status',
        executor: makeLocalMcpExecutor(),
        state: 'completed',
        started_at_ms: NOW_MS - 2_000,
        finished_at_ms: NOW_MS - 1_000,
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS, windowHours: 24 });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.updatedAtMs, NOW_MS - 1_000);
  assert.equal(session.stateLabel, '5 older plans awaiting approval');
  assert.equal(session.olderJobCount, 5);
  assert.equal(session.attentionItems.length, 5);
  for (const item of session.attentionItems) {
    assert.equal(item.kind, 'plan_approval');
    assert.equal(item.olderThanWindow, true);
    assert.equal(item.reason, 'Plan awaiting approval');
    assert.ok(item.updatedAtMs !== null && item.updatedAtMs <= nov6Ms + 10_000);
  }
  for (const jobView of session.jobs) {
    assert.equal(jobView.stateLabel, 'Awaiting approval');
    assert.equal(jobView.attentionKind, 'plan_approval');
    assert.equal(jobView.olderThanWindow, true);
  }
});

test('held work outranks pending approval and mixed reasons summarize cleanly without invented causes', () => {
  const oldMs = NOW_MS - 30 * ONE_HOUR_MS;
  const heldVsApprovalSnapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-held-plan',
        title: 'Held migration plan',
        plan_id: 'plan-held',
        state: 'needs_user',
        approval_status: 'awaiting_approval',
        execution_status: 'on_hold',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 30_000,
      }),
    ],
  });
  const heldProjection = projectProjects(heldVsApprovalSnapshot, { nowMs: NOW_MS });
  const heldSession = heldProjection.projects[0]?.sessions[0];
  assert.ok(heldSession);
  assert.equal(heldSession.stateLabel, 'Work on hold');
  assert.equal(heldSession.jobs[0]?.attentionKind, 'on_hold');
  assert.equal(heldSession.jobs[0]?.stateLabel, 'On hold');
  assert.equal(heldSession.jobs[0]?.reason, 'Work on hold');

  const mixedSnapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-mix-approval',
        title: 'Plan needing approval',
        plan_id: 'plan-10',
        state: 'ready',
        approval_status: 'pending_approval',
        execution_status: 'pending',
        active_call_count: 0,
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-mix-generic-wait',
        title: 'Unqualified wait job',
        plan_id: null,
        state: 'needs_user',
        approval_status: 'none',
        execution_status: 'waiting_for_user',
        active_call_count: 0,
        updated_at_ms: oldMs - 5_000,
      }),
      makeJob({
        id: 'job-mix-unknown',
        title: 'Conflicting old test',
        plan_id: null,
        state: 'unknown',
        approval_status: 'unknown',
        execution_status: 'unknown',
        active_call_count: 0,
        updated_at_ms: oldMs - 10_000,
      }),
    ],
  });
  const mixedProjection = projectProjects(mixedSnapshot, { nowMs: NOW_MS });
  const mixedSession = mixedProjection.projects[0]?.sessions[0];
  assert.ok(mixedSession);
  assert.equal(mixedSession.stateLabel, '3 older items to check');
  assert.equal(mixedSession.tone, 'unknown');
  assert.deepEqual(
    mixedSession.attentionItems.map((item) => item.kind),
    ['status_check', 'plan_approval', 'recorded_wait']
  );
  const genericWait = mixedSession.attentionItems.find(
    (item) => item.jobId === 'job-mix-generic-wait'
  );
  assert.equal(genericWait?.reason, 'Recorded wait; reason not provided');
  const statusCheck = mixedSession.attentionItems.find(
    (item) => item.jobId === 'job-mix-unknown'
  );
  assert.equal(statusCheck?.reason, 'Status needs checking');
});

test('settled completed and failed jobs with leftover pending fields do not regain approval and stay source-isolated', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-1', label: 'Alpha', project_path: '/work/alpha' }),
      makeSource({ id: 'src-2', label: 'Beta', project_path: '/work/beta' }),
    ],
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-shared',
        project_label: 'Alpha',
        project_path: '/work/alpha',
        title: 'Completed with leftover approval',
        plan_id: 'plan-leftover',
        state: 'completed',
        approval_status: 'awaiting_approval',
        execution_status: 'on_hold',
        review_state: 'ready_for_review',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 10_000,
      }),
      makeJob({
        source_id: 'src-1',
        id: 'job-failed-recent',
        project_label: 'Alpha',
        project_path: '/work/alpha',
        thread_uuid: 'thread-failed',
        binding_id: 'bind-failed',
        title: 'Failed with leftover approval',
        plan_id: 'plan-failed',
        state: 'failed',
        approval_status: 'pending_approval',
        execution_status: 'failed',
        review_state: 'needs_review',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 20_000,
      }),
      makeJob({
        source_id: 'src-1',
        id: 'job-failed-old',
        project_label: 'Alpha',
        project_path: '/work/alpha',
        thread_uuid: 'thread-failed-old',
        binding_id: 'bind-failed-old',
        title: 'Historical failed job',
        plan_id: 'plan-failed-old',
        state: 'failed',
        approval_status: 'awaiting_approval',
        execution_status: 'failed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
      }),
      makeJob({
        source_id: 'src-2',
        id: 'job-shared',
        project_label: 'Beta',
        project_path: '/work/beta',
        title: 'Pending in second source',
        plan_id: 'plan-beta',
        state: 'ready',
        approval_status: 'approval_needed',
        execution_status: 'pending',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 15_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-settled-leftover',
        job_id: 'job-shared',
        project_label: 'Alpha',
        state: 'waiting_for_user',
        updated_at_ms: NOW_MS - 9_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.hiddenSessionCount, 1);
  const alphaProject = projection.projects.find((p) => p.label === 'Alpha');
  const betaProject = projection.projects.find((p) => p.label === 'Beta');
  assert.ok(alphaProject);
  assert.ok(betaProject);

  const completedSession = alphaProject.sessions.find(
    (s) => s.threadUuid === 'thread-1'
  );
  assert.ok(completedSession);
  assert.equal(completedSession.stateLabel, 'Completed');
  assert.equal(completedSession.attention, false);
  assert.equal(completedSession.attentionItems.length, 0);
  assert.equal(completedSession.jobs[0]?.attention, false);

  const failedSession = alphaProject.sessions.find(
    (s) => s.threadUuid === 'thread-failed'
  );
  assert.ok(failedSession);
  assert.equal(failedSession.stateLabel, 'Failed');
  assert.equal(failedSession.attentionItems.length, 0);
  assert.equal(failedSession.jobs[0]?.attention, false);

  const betaSession = betaProject.sessions[0];
  assert.ok(betaSession);
  assert.equal(betaSession.stateLabel, '1 plan awaiting approval');
  assert.equal(betaSession.attentionItems[0]?.sourceId, 'src-2');
  assert.equal(betaSession.attentionItems[0]?.jobId, 'job-shared');
});

test('waiting request without job uses actual request identity and deduplicates parent-child and job waits', () => {
  const oldWaitMs = NOW_MS - 12 * ONE_HOUR_MS;
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-with-wait',
        title: 'Job already waiting',
        thread_uuid: 'thread-job-wait',
        binding_id: 'bind-job-wait',
        plan_id: null,
        state: 'needs_user',
        approval_status: 'none',
        execution_status: 'waiting_for_user',
        active_call_count: 0,
        updated_at_ms: oldWaitMs,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-parent-wait',
        job_id: null,
        thread_uuid: 'thread-req-only',
        binding_id: 'bind-req-only',
        title: 'Parent handoff wait',
        state: 'waiting_for_user',
        started_at_ms: oldWaitMs - 5_000,
        finished_at_ms: null,
        updated_at_ms: oldWaitMs,
      }),
      makeRequest({
        id: 'req-child-wait',
        parent_request_id: 'req-parent-wait',
        job_id: null,
        thread_uuid: 'thread-req-only',
        binding_id: 'bind-req-only',
        title: 'Confirm target environment',
        state: 'waiting_for_user',
        started_at_ms: oldWaitMs - 2_000,
        finished_at_ms: null,
        updated_at_ms: oldWaitMs,
        children: [
          makeCall({
            id: 'call-child-wait',
            title: 'Confirm target environment',
            state: 'waiting_for_user',
            started_at_ms: oldWaitMs - 2_000,
            finished_at_ms: null,
          }),
        ],
        children_count: 1,
      }),
      makeRequest({
        id: 'req-duplicate-of-job',
        job_id: 'job-with-wait',
        thread_uuid: 'thread-job-wait',
        binding_id: 'bind-job-wait',
        title: 'Duplicate request wait for job',
        state: 'waiting_for_user',
        updated_at_ms: oldWaitMs,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const sessions = projection.projects[0]?.sessions ?? [];
  const reqOnlySession = sessions.find((s) => s.threadUuid === 'thread-req-only');
  assert.ok(reqOnlySession);
  assert.equal(reqOnlySession.jobs.length, 0);
  assert.equal(reqOnlySession.attentionItems.length, 1);
  assert.equal(reqOnlySession.attentionItems[0]?.jobId, null);
  assert.equal(reqOnlySession.attentionItems[0]?.requestId, 'req-child-wait');
  assert.equal(reqOnlySession.attentionItems[0]?.title, 'Confirm target environment');
  assert.equal(reqOnlySession.attentionItems[0]?.updatedAtMs, oldWaitMs);
  assert.equal(reqOnlySession.attentionItems[0]?.olderThanWindow, true);
  assert.equal(reqOnlySession.stateLabel, 'Waiting for user (older)');

  const jobWaitSession = sessions.find((s) => s.threadUuid === 'thread-job-wait');
  assert.ok(jobWaitSession);
  assert.equal(jobWaitSession.attentionItems.length, 1);
  assert.equal(jobWaitSession.attentionItems[0]?.jobId, 'job-with-wait');
});

test('persisted reviewing or ready_for_review without active reviewer reports awaiting review while running reviewer stays running', () => {
  const oldReviewMs = NOW_MS - 18 * ONE_HOUR_MS;
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-stale-review',
        thread_uuid: 'thread-stale-review',
        binding_id: 'bind-stale-review',
        title: 'Worker finished awaiting review',
        state: 'reviewing',
        review_state: 'ready_for_review',
        active_call_count: 0,
        updated_at_ms: oldReviewMs,
      }),
      makeJob({
        id: 'job-active-review',
        thread_uuid: 'thread-active-review',
        binding_id: 'bind-active-review',
        title: 'Active package review',
        state: 'reviewing',
        review_state: 'reviewing',
        active_call_count: 1,
        updated_at_ms: NOW_MS - 3_000,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-stale-worker',
        job_id: 'job-stale-review',
        thread_uuid: 'thread-stale-review',
        binding_id: 'bind-stale-review',
        title: 'Worker handoff',
        state: 'ready_for_review',
        started_at_ms: oldReviewMs - 10_000,
        finished_at_ms: oldReviewMs,
        updated_at_ms: oldReviewMs,
      }),
      makeRequest({
        id: 'req-active-reviewer',
        job_id: 'job-active-review',
        thread_uuid: 'thread-active-review',
        binding_id: 'bind-active-review',
        title: 'Closing review',
        state: 'running',
        executor: makeExecutor({ role: 'reviewer', requested_model: 'gpt-6-astra', observed_model: 'gpt-6-astra' }),
        started_at_ms: NOW_MS - 3_000,
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const sessions = projection.projects[0]?.sessions ?? [];
  const staleSession = sessions.find((s) => s.threadUuid === 'thread-stale-review');
  assert.ok(staleSession);
  assert.equal(staleSession.runningCount, 0);
  assert.equal(staleSession.stateLabel, 'Older work awaiting review');
  assert.equal(staleSession.tone, 'review');
  assert.equal(staleSession.jobs[0]?.stateLabel, 'Awaiting review');
  assert.equal(staleSession.attentionItems.length, 1);
  assert.match(staleSession.attentionItems[0]?.reason ?? '', /no active reviewer/i);

  const activeSession = sessions.find((s) => s.threadUuid === 'thread-active-review');
  assert.ok(activeSession);
  assert.equal(activeSession.runningCount, 1);
  assert.equal(activeSession.stateLabel, 'Running');
  assert.equal(activeSession.tone, 'active');
  assert.equal(activeSession.jobs[0]?.stateLabel, 'Reviewing');
  assert.equal(activeSession.jobs[0]?.attention, false);
  assert.equal(activeSession.attentionItems.length, 0);
});

test('reviewer running identity is scoped to exact source+job, stale planning shows Planning recorded, and task status summary is grouped', () => {
  const oldMs = NOW_MS - 48 * ONE_HOUR_MS;
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-a',
        thread_uuid: 'thread-mixed-review',
        thread_label: 'Mixed Review Session',
        state: 'reviewing',
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJob({
        id: 'job-b',
        thread_uuid: 'thread-mixed-review',
        thread_label: 'Mixed Review Session',
        state: 'reviewing',
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-c',
        thread_uuid: 'thread-worker-on-review',
        thread_label: 'Worker On Reviewing Job',
        state: 'reviewing',
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJob({
        id: 'job-unlinked',
        thread_uuid: 'thread-unlinked-reviewer',
        thread_label: 'Unlinked Reviewer Session',
        state: 'reviewing',
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJob({
        id: 'job-stale-plan',
        thread_uuid: 'thread-stale-plan',
        thread_label: 'Stale Planning Session',
        state: 'planning',
        updated_at_ms: oldMs,
        tasks: [
          { id: 't1', title: 'Task done', status: 'completed', depends_on: [] },
          { id: 't2', title: 'Task review', status: 'ready_for_review', depends_on: [] },
          { id: 't3', title: 'Task failed', status: 'failed', depends_on: [] },
          { id: 't4', title: 'Task waiting', status: 'not_started', depends_on: [] },
        ],
      }),
      makeJob({
        id: 'job-held',
        thread_uuid: 'thread-held',
        thread_label: 'Held Session',
        state: 'needs_user',
        execution_status: 'held',
        updated_at_ms: NOW_MS - 5_000,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-reviewer-a',
        job_id: 'job-a',
        thread_uuid: 'thread-mixed-review',
        thread_label: 'Mixed Review Session',
        state: 'running',
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        executor: makeExecutor({ role: 'reviewer' }),
      }),
      makeRequest({
        id: 'req-worker-c',
        job_id: 'job-c',
        thread_uuid: 'thread-worker-on-review',
        thread_label: 'Worker On Reviewing Job',
        state: 'running',
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        executor: makeExecutor({ role: 'coder' }),
      }),
      makeRequest({
        id: 'req-reviewer-unlinked',
        job_id: null,
        thread_uuid: 'thread-unlinked-reviewer',
        thread_label: 'Unlinked Reviewer Session',
        state: 'running',
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        executor: makeExecutor({ role: 'Reviewer' }),
      }),
      makeRequest({
        id: 'req-mcp-heartbeat',
        job_id: 'job-stale-plan',
        thread_uuid: 'thread-stale-plan',
        thread_label: 'Stale Planning Session',
        state: 'running',
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        executor: makeLocalMcpExecutor(),
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const sessions = projection.projects[0]?.sessions ?? [];

  const mixed = sessions.find((s) => s.threadUuid === 'thread-mixed-review');
  assert.ok(mixed);
  const jobA = mixed.jobs.find((j) => j.job.id === 'job-a');
  const jobB = mixed.jobs.find((j) => j.job.id === 'job-b');
  assert.equal(jobA?.stateLabel, 'Reviewing');
  assert.equal(jobA?.attention, false);
  assert.equal(jobB?.stateLabel, 'Awaiting review');
  assert.equal(jobB?.attention, true);
  assert.equal(jobB?.olderThanWindow, true);

  const workerOnReview = sessions.find((s) => s.threadUuid === 'thread-worker-on-review');
  assert.ok(workerOnReview);
  assert.equal(workerOnReview.jobs[0]?.stateLabel, 'Awaiting review');
  assert.equal(workerOnReview.jobs[0]?.attention, true);

  const unlinked = sessions.find((s) => s.threadUuid === 'thread-unlinked-reviewer');
  assert.ok(unlinked);
  assert.equal(unlinked.runningCount, 1);
  assert.equal(unlinked.jobs[0]?.stateLabel, 'Awaiting review');
  assert.equal(unlinked.jobs[0]?.attention, true);

  const stalePlan = sessions.find((s) => s.threadUuid === 'thread-stale-plan');
  assert.ok(stalePlan);
  assert.equal(stalePlan.jobs[0]?.stateLabel, 'Planning recorded');
  assert.equal(stalePlan.jobs[0]?.attention, true);
  assert.equal(
    stalePlan.jobs[0]?.reason,
    'Planning was recorded; no active planning call is verified'
  );
  assert.equal(stalePlan.jobs[0]?.updatedAtMs, oldMs);
  assert.equal(stalePlan.jobs[0]?.olderThanWindow, true);
  assert.equal(
    stalePlan.jobs[0]?.taskSummary,
    '1 failed · 1 awaiting review · 1 not started · 1 done'
  );

  const held = sessions.find((s) => s.threadUuid === 'thread-held');
  assert.ok(held);
  assert.equal(
    held.jobs[0]?.nextStep,
    'Review the hold in Codex; updated scope or approval may be needed.'
  );
});

test('nested running Reviewer under a local MCP parent only suppresses its exact source and job', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource(),
      makeSource({
        id: 'src-2',
        label: 'Other',
        project_path: '/work/other',
      }),
    ],
    jobs: [
      makeJob({
        id: 'job-nested-review',
        thread_uuid: 'thread-nested-review',
        thread_label: 'Nested Review Session',
        state: 'running',
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJob({
        id: 'job-other',
        thread_uuid: 'thread-nested-review',
        thread_label: 'Nested Review Session',
        state: 'running',
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJob({
        source_id: 'src-2',
        id: 'job-nested-review',
        project_label: 'Other',
        project_path: '/work/other',
        thread_uuid: 'thread-other-source',
        thread_label: 'Other Source Session',
        state: 'running',
        updated_at_ms: NOW_MS - 2_000,
      }),
      makeJob({
        id: 'job-unbound-nested',
        thread_uuid: 'thread-unbound-nested',
        thread_label: 'Unbound Nested Session',
        state: 'running',
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-nested-parent',
        job_id: 'job-nested-review',
        thread_uuid: 'thread-nested-review',
        thread_label: 'Nested Review Session',
        state: 'running',
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        executor: makeLocalMcpExecutor(),
        children: [
          makeCall({
            id: 'call-nested-reviewer',
            state: 'running',
            executor: makeExecutor({ role: 'reviewer' }),
            finished_at_ms: null,
          }),
        ],
        children_count: 1,
      }),
      makeRequest({
        id: 'req-nested-ready',
        job_id: 'job-nested-review',
        thread_uuid: 'thread-nested-review',
        thread_label: 'Nested Review Session',
        title: 'Nested review ready',
        state: 'ready_for_review',
        finished_at_ms: NOW_MS - 4_000,
        updated_at_ms: NOW_MS - 4_000,
      }),
      makeRequest({
        id: 'req-other-job-ready',
        job_id: 'job-other',
        thread_uuid: 'thread-nested-review',
        thread_label: 'Nested Review Session',
        title: 'Other job review ready',
        state: 'ready_for_review',
        finished_at_ms: NOW_MS - 4_000,
        updated_at_ms: NOW_MS - 4_000,
      }),
      makeRequest({
        id: 'req-other-source-ready',
        source_id: 'src-2',
        job_id: 'job-nested-review',
        thread_uuid: 'thread-other-source',
        thread_label: 'Other Source Session',
        title: 'Other source review ready',
        state: 'ready_for_review',
        finished_at_ms: NOW_MS - 4_000,
        updated_at_ms: NOW_MS - 4_000,
      }),
      makeRequest({
        id: 'req-unbound-parent',
        job_id: null,
        thread_uuid: 'thread-unbound-nested',
        thread_label: 'Unbound Nested Session',
        state: 'running',
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        executor: makeLocalMcpExecutor(),
        children: [
          makeCall({
            id: 'call-unbound-reviewer',
            state: 'running',
            executor: makeExecutor({ role: 'reviewer' }),
            finished_at_ms: null,
          }),
        ],
        children_count: 1,
      }),
      makeRequest({
        id: 'req-unbound-ready',
        job_id: null,
        thread_uuid: 'thread-unbound-nested',
        thread_label: 'Unbound Nested Session',
        title: 'Unbound review ready',
        state: 'ready_for_review',
        finished_at_ms: NOW_MS - 4_000,
        updated_at_ms: NOW_MS - 4_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const sessions = projection.projects.flatMap((project) => project.sessions);

  const nested = sessions.find(
    (session) => session.threadUuid === 'thread-nested-review'
  );
  assert.ok(nested);
  assert.equal(
    nested.attentionItems.some((item) => item.requestId === 'req-nested-ready'),
    false
  );
  assert.equal(
    nested.attentionItems.some((item) => item.requestId === 'req-other-job-ready'),
    true
  );

  const otherSource = sessions.find(
    (session) => session.threadUuid === 'thread-other-source'
  );
  assert.ok(otherSource);
  assert.equal(
    otherSource.attentionItems.some(
      (item) => item.requestId === 'req-other-source-ready'
    ),
    true
  );

  const unbound = sessions.find(
    (session) => session.threadUuid === 'thread-unbound-nested'
  );
  assert.ok(unbound);
  assert.equal(
    unbound.attentionItems.some((item) => item.requestId === 'req-unbound-ready'),
    true
  );
});

test('unbound Reviewer does not suppress an unrelated pending review', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-unbound-context',
        thread_uuid: 'thread-unbound-review',
        thread_label: 'Unbound Reviewer Session',
        state: 'completed',
        execution_status: 'completed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 6_000,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-pending-review',
        job_id: null,
        thread_uuid: 'thread-unbound-review',
        thread_label: 'Unbound Reviewer Session',
        title: 'Pending review',
        state: 'ready_for_review',
        started_at_ms: NOW_MS - 8_000,
        finished_at_ms: NOW_MS - 4_000,
        updated_at_ms: NOW_MS - 4_000,
      }),
      makeRequest({
        id: 'req-unbound-reviewer',
        job_id: null,
        thread_uuid: 'thread-unbound-review',
        thread_label: 'Unbound Reviewer Session',
        title: 'Unbound review',
        state: 'running',
        executor: makeExecutor({ role: 'Reviewer' }),
        started_at_ms: NOW_MS - 3_000,
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions.find(
    (candidate) => candidate.threadUuid === 'thread-unbound-review'
  );
  assert.ok(session);
  assert.equal(session.attentionItems.length, 1);
  assert.equal(session.attentionItems[0]?.requestId, 'req-pending-review');
  assert.equal(session.attentionItems[0]?.kind, 'awaiting_review');
});

test('needs_user with held execution status retains hold guidance', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-held-alias',
        thread_uuid: 'thread-held-alias',
        thread_label: 'Held Alias Session',
        state: 'needs_user',
        execution_status: 'held',
        updated_at_ms: NOW_MS - 5_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions.find(
    (candidate) => candidate.threadUuid === 'thread-held-alias'
  );
  assert.ok(session);
  assert.equal(
    session.jobs[0]?.nextStep,
    'Review the hold in Codex; updated scope or approval may be needed.'
  );
});
